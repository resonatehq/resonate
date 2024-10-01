package dst

import (
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"strconv"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/task"
)

type DST struct {
	config    *Config
	generator *Generator
	validator *Validator
}

type Config struct {
	Ticks              int64
	Timeout            time.Duration
	VisualizationPath  string
	TimeElapsedPerTick int64
	TimeoutTicks       int64
	ReqsPerTick        func() int
	MaxReqsPerTick     int64
	Ids                int
	IdempotencyKeys    int
	Headers            int
	Data               int
	Tags               int
	FaultInjection     bool
	Backchannel        chan interface{}
}

type Kind int

const (
	Op Kind = iota
	Bc
)

type Req struct {
	kind Kind
	time int64
	req  *t_api.Request
	bc   *Backchannel
}

type Res struct {
	kind Kind
	time int64
	res  *t_api.Response
	err  error
}

type Backchannel struct {
	Task *task.Task
}

func New(r *rand.Rand, config *Config) *DST {
	return &DST{
		config:    config,
		generator: NewGenerator(r, config),
		validator: NewValidator(r, config),
	}
}

func (d *DST) Add(kind t_api.Kind, generator RequestGenerator, validator ResponseValidator) {
	d.generator.AddGenerator(kind, generator)
	d.validator.AddValidator(kind, validator)
}

func (d *DST) Run(r *rand.Rand, api api.API, aio aio.AIO, system *system.System) bool {
	util.Assert(d.config.Backchannel != nil, "backchannel must be non nil")

	// promises
	d.Add(t_api.ReadPromise, d.generator.GenerateReadPromise, d.validator.ValidateReadPromise)
	d.Add(t_api.SearchPromises, d.generator.GenerateSearchPromises, d.validator.ValidateSearchPromises)
	d.Add(t_api.CreatePromise, d.generator.GenerateCreatePromise, d.validator.ValidateCreatePromise)
	d.Add(t_api.CreatePromiseAndTask, d.generator.GenerateCreatePromiseAndTask, d.validator.ValidateCreatePromiseAndTask)
	d.Add(t_api.CreatePromiseAndCallback, d.generator.GenerateCreatePromiseAndCallback, d.validator.ValidateCreatePromiseAndCallback)
	d.Add(t_api.CompletePromise, d.generator.GenerateCompletePromise, d.validator.ValidateCompletePromise)

	// callbacks
	d.Add(t_api.CreateCallback, d.generator.GenerateCreateCallback, d.validator.ValidateCreateCallback)

	// schedules
	d.Add(t_api.ReadSchedule, d.generator.GenerateReadSchedule, d.validator.ValidateReadSchedule)
	d.Add(t_api.SearchSchedules, d.generator.GenerateSearchSchedules, d.validator.ValidateSearchSchedules)
	d.Add(t_api.CreateSchedule, d.generator.GenerateCreateSchedule, d.validator.ValidateCreateSchedule)
	d.Add(t_api.DeleteSchedule, d.generator.GenerateDeleteSchedule, d.validator.ValidateDeleteSchedule)

	// locks
	d.Add(t_api.AcquireLock, d.generator.GenerateAcquireLock, d.validator.ValidateAcquireLock)
	d.Add(t_api.ReleaseLock, d.generator.GenerateReleaseLock, d.validator.ValidateReleaseLock)
	d.Add(t_api.HeartbeatLocks, d.generator.GenerateHeartbeatLocks, d.validator.ValidateHeartbeatLocks)

	// tasks
	d.Add(t_api.ClaimTask, d.generator.GenerateClaimTask, d.validator.ValidateClaimTask)
	d.Add(t_api.CompleteTask, d.generator.GenerateCompleteTask, d.validator.ValidateCompleteTask)
	d.Add(t_api.HeartbeatTasks, d.generator.GenerateHeartbeatTasks, d.validator.ValidateHeartbeatTasks)

	// porcupine ops
	var ops []porcupine.Operation

	// run all requests through the server and collect responses
	var t, i, j int64
	for t = int64(0); t < d.config.Ticks; t++ {
		time := d.Time(t)

		for _, req := range d.generator.Generate(r, time, d.config.ReqsPerTick()) {
			id := strconv.FormatInt(i, 10)
			req := req
			reqTime := time

			req.Tags = map[string]string{
				"id":   id,
				"name": req.Kind.String(),
			}

			api.EnqueueSQE(&bus.SQE[t_api.Request, t_api.Response]{
				Submission: req,
				Callback: func(res *t_api.Response, err error) {
					resTime := d.Time(t)
					if reqTime != resTime {
						resTime = resTime - 1 // subtract 1 to ensure tick timeframes don't overlap
					}

					// log
					slog.Info("DST", "t", fmt.Sprintf("%d|%d", reqTime, resTime), "id", id, "req", req, "res", res, "err", err)

					// extract cursors for subsequent requests
					if err == nil {
						switch res.Kind {
						case t_api.SearchPromises:
							if res.SearchPromises.Cursor != nil {
								d.generator.AddRequest(&t_api.Request{
									Kind:           t_api.SearchPromises,
									SearchPromises: res.SearchPromises.Cursor.Next,
								})
							}
						case t_api.SearchSchedules:
							if res.SearchSchedules.Cursor != nil {
								d.generator.AddRequest(&t_api.Request{
									Kind:            t_api.SearchSchedules,
									SearchSchedules: res.SearchSchedules.Cursor.Next,
								})
							}
						}
					}

					// add operation to porcupine
					ops = append(ops, porcupine.Operation{
						ClientId: int(j % d.config.MaxReqsPerTick),
						Call:     reqTime,
						Return:   resTime,
						Input:    &Req{Op, reqTime, req, nil},
						Output:   &Res{Op, resTime, res, err},
					})

					// Warning:
					// A CreatePromiseAndTask request applies to two partitions, the
					// promise partition and the task partition. Merging the
					// partitions results in long checking time, so as a workaround
					// we create an independent CreatePromise request. The mapping
					// of requests to partitions is as follows:
					// CreatePromise        -> p partition
					// CreatePromiseAndTask -> t partition
					//
					// Note:
					// A similar approach is not needed for CreatePromiseAndCallback
					// because callbacks and promises are part of the same
					// partition.
					if req.Kind == t_api.CreatePromiseAndTask {
						j++

						req = &t_api.Request{
							Kind:          t_api.CreatePromise,
							Tags:          req.Tags,
							CreatePromise: req.CreatePromiseAndTask.Promise,
						}

						if res != nil {
							res = &t_api.Response{
								Kind: t_api.CreatePromise,
								Tags: res.Tags,
								CreatePromise: &t_api.CreatePromiseResponse{
									Status:  res.CreatePromiseAndTask.Status,
									Promise: res.CreatePromiseAndTask.Promise,
								},
							}
						}

						ops = append(ops, porcupine.Operation{
							ClientId: int(j % d.config.MaxReqsPerTick),
							Call:     reqTime,
							Return:   resTime,
							Input:    &Req{Op, reqTime, req, nil},
							Output:   &Res{Op, resTime, res, err},
						})
					}

					j++
				},
			})
			i++
		}

		system.Tick(time)

		// now read from the callback channel
		for len(d.config.Backchannel) > 0 {
			var bc *Backchannel
			obj := <-d.config.Backchannel

			switch obj := obj.(type) {
			case *task.Task:
				bc = &Backchannel{
					Task: obj,
				}

				// randomly decrement the counter, we only decrement so that we
				// know a successful claim task request can only occur after
				// our model has been updated via the backchannel
				counter := obj.Counter - r.Intn(2)

				// add claim req to generator
				d.generator.AddRequest(&t_api.Request{
					Kind: t_api.ClaimTask,
					ClaimTask: &t_api.ClaimTaskRequest{
						Id:        obj.Id,
						Counter:   counter,
						ProcessId: obj.Id,
						Frequency: RangeIntn(r, 1000, 5000),
					},
				})
			default:
				panic("invalid backchannel type")
			}

			// backchannel messages occur on the "last" tick
			reqTime := d.Time(t - 1)
			resTime := time - 1

			// add backchannel op to porcupine
			ops = append(ops, porcupine.Operation{
				ClientId: int(j % d.config.MaxReqsPerTick),
				Call:     reqTime,
				Return:   resTime,
				Input:    &Req{Bc, reqTime, nil, bc},
				Output:   &Res{Bc, resTime, nil, nil},
			})

			j++
		}
	}

	// shutdown the system
	system.Shutdown()

	// keep ticking until all submissions have been processed
	for !system.Done() {
		t++
		system.Tick(d.Time(t))
	}

	if d.config.FaultInjection {
		slog.Info("Skipping linearization check because DST was run with fault injections")
		return true
	}

	model := d.Model()
	result, history := porcupine.CheckOperationsVerbose(model, ops, d.config.Timeout)

	if err := porcupine.VisualizePath(model, history, d.config.VisualizationPath); err != nil {
		slog.Error("failed to create visualization", "err", err)
		return false
	}

	switch result {
	case porcupine.Ok:
		slog.Info("DST is linearizable")
	case porcupine.Illegal:
		slog.Error("DST is non linearizable")
	case porcupine.Unknown:
		slog.Error("DST timed out before linearizability could be determined")
	}

	return result == porcupine.Ok
}

func (d *DST) Model() porcupine.Model {
	return porcupine.Model{
		Init: func() interface{} {
			return NewModel()
		},
		Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
			p := []porcupine.Operation{}
			s := []porcupine.Operation{}
			l := []porcupine.Operation{}
			t := []porcupine.Operation{}

			for _, op := range history {
				req := op.Input.(*Req)

				switch req.kind {
				case Op:
					switch req.req.Kind {
					case t_api.ReadPromise, t_api.SearchPromises, t_api.CreatePromise, t_api.CreatePromiseAndCallback, t_api.CompletePromise, t_api.CreateCallback:
						p = append(p, op)
					case t_api.ReadSchedule, t_api.SearchSchedules, t_api.CreateSchedule, t_api.DeleteSchedule:
						s = append(s, op)
					case t_api.AcquireLock, t_api.ReleaseLock, t_api.HeartbeatLocks:
						l = append(l, op)
					case t_api.ClaimTask, t_api.CompleteTask, t_api.HeartbeatTasks, t_api.CreatePromiseAndTask:
						t = append(t, op)
					default:
						panic(fmt.Sprintf("unknown request kind: %s", req.req.Kind))
					}
				case Bc:
					if req.bc.Task != nil {
						t = append(t, op)
					}
				default:
					panic(fmt.Sprintf("unknown request kind: %d", req.kind))
				}
			}

			return [][]porcupine.Operation{p, s, l, t}
		},
		Step: func(state, input, output interface{}) (bool, interface{}) {
			model := state.(*Model)
			req := input.(*Req)
			res := output.(*Res)

			util.Assert(req.kind == res.kind, "kinds must match ")

			switch req.kind {
			case Op:
				updatedModel, err := d.Step(model, req.time, res.time, req.req, res.res, res.err)
				if err != nil {
					return false, model
				}
				return true, updatedModel
			case Bc:
				updatedModel := model.Copy()
				if req.bc.Task != nil {
					updatedModel.tasks.set(req.bc.Task.Id, req.bc.Task)
				}
				return true, updatedModel
			default:
				panic(fmt.Sprintf("unknown request kind: %d", req.kind))
			}
		},
		Equal: func(state1, state2 interface{}) bool {
			model1 := state1.(*Model)
			model2 := state2.(*Model)

			return model1 == model2 || model1.Equals(model2)
		},
		DescribeOperation: func(input interface{}, output interface{}) string {
			req := input.(*Req)
			res := output.(*Res)

			switch req.kind {
			case Op:
				var status int
				if res.err != nil {
					var err *t_api.Error
					if errors.As(res.err, &err) {
						status = int(err.Code())
					}
				} else {
					status = int(res.res.Status())
				}

				return fmt.Sprintf("%s | %s → %d", req.req.Tags["id"], req.req, status)
			case Bc:
				return fmt.Sprintf("Backchannel | %s", req.bc.Task)
			default:
				panic(fmt.Sprintf("unknown request kind: %d", req.kind))
			}
		},
		DescribeState: func(state interface{}) string {
			model := state.(*Model)

			switch {
			case len(*model.promises) > 0 || len(*model.callbacks) > 0:
				var promises string
				for _, p := range *model.promises {
					promises = promises + fmt.Sprintf(`
					<tr>
						<td align="right">%s</td>
						<td>%s</td>
						<td align="right">%s</td>
						<td align="right">%s</td>
						<td align="right">%d</td>
					</tr>
				`, p.value.Id, p.value.State, p.value.IdempotencyKeyForCreate, p.value.IdempotencyKeyForComplete, p.value.Timeout)
				}

				var callbacks string
				for _, c := range *model.callbacks {
					callbacks = callbacks + fmt.Sprintf(`
					<tr>
						<td align="right">%s</td>
						<td align="right">%s</td>
					</tr>
				`, c.value.Id, c.value.PromiseId)
				}

				return fmt.Sprintf(`
					<table border="0" cellspacing="0" cellpadding="5">
						<thead>
							<tr>
								<td><b>Promises</b></td>
								<td><b>Callbacks</b></td>
							</tr>
						</thead>
						<tbody>
							<tr>
								<td valign="top">
									<table border="1" cellspacing="0" cellpadding="5">
										<thead>
											<tr>
												<td><b>id</b></td>
												<td><b>state</b></td>
												<td><b>ikeyCreate</b></td>
												<td><b>ikeyComplete</b></td>
												<td><b>timeout</b></td>
											</tr>
										</thead>
										<tbody>
											%s
										</tbody>
									</table>
								</td>
								<td valign="top">
									<table border="1" cellspacing="0" cellpadding="5">
										<thead>
											<tr>
												<td><b>id</b></td>
												<td><b>promiseId</b></td>
											</tr>
										</thead>
										<tbody>
											%s
										</tbody>
									</table>
								</td>
							</tr>
						</tbody>
					</table>
				`, promises, callbacks)
			case len(*model.schedules) > 0:
				var schedules string
				for _, s := range *model.schedules {
					schedules = schedules + fmt.Sprintf(`
					<tr>
						<td align="right">%s</td>
						<td align="right">%s</td>
					</tr>
				`, s.value.Id, s.value.IdempotencyKey)
				}

				return fmt.Sprintf(`
					<table border="0" cellspacing="0" cellpadding="5">
						<thead>
							<tr>
								<td><b>Schedules</b></td>
							</tr>
						</thead>
						<tbody>
							<tr>
								<td valign="top">
									<table border="1" cellspacing="0" cellpadding="5">
										<thead>
											<tr>
												<td><b>id</b></td>
												<td><b>ikey</b></td>
											</tr>
										</thead>
										<tbody>
											%s
										</tbody>
									</table>
								</td>
							</tr>
						</tbody>
					</table>
				`, schedules)
			case len(*model.locks) > 0:
				var locks string
				for _, s := range *model.locks {
					locks = locks + fmt.Sprintf(`
					<tr>
						<td align="right">%s</td>
						<td align="right">%s</td>
						<td align="right">%s</td>
						<td align="right">%d</td>
					</tr>
				`, s.value.ResourceId, s.value.ExecutionId, s.value.ProcessId, s.value.ExpiresAt)
				}

				return fmt.Sprintf(`
					<table border="0" cellspacing="0" cellpadding="5">
						<thead>
							<tr>
								<td><b>Locks</b></td>
							</tr>
						</thead>
						<tbody>
							<tr>
								<td valign="top">
									<table border="1" cellspacing="0" cellpadding="5">
										<thead>
											<tr>
												<td><b>rid</b></td>
												<td><b>eid</b></td>
												<td><b>pid</b></td>
												<td><b>timeout</b></td>
											</tr>
										</thead>
										<tbody>
											%s
										</tbody>
									</table>
								</td>
							</tr>
						</tbody>
					</table>
				`, locks)
			case len(*model.tasks) > 0:
				var tasks string
				for _, t := range *model.tasks {
					tasks = tasks + fmt.Sprintf(`
						<tr>
							<td align="right">%s</td>
							<td align="right">%s</td>
							<td>%s</td>
							<td align="right">%d</td>
							<td align="right">%d</td>
							<td align="right">%d</td>
						</tr>
					`, t.value.Id, util.SafeDeref(t.value.ProcessId), t.value.State, t.value.Counter, t.value.Expiration, t.value.Timeout)
				}

				return fmt.Sprintf(`
						<table border="0" cellspacing="0" cellpadding="5">
							<thead>
								<tr>
									<td><b>Tasks</b></td>
								</tr>
							</thead>
							<tbody>
								<tr>
									<td valign="top">
										<table border="1" cellspacing="0" cellpadding="5">
											<thead>
												<tr>
													<td><b>id</b></td>
													<td><b>processId</b></td>
													<td><b>state</b></td>
													<td><b>counter</b></td>
													<td><b>expiration</b></td>
													<td><b>timeout</b></td>
												</tr>
											</thead>
											<tbody>
												%s
											</tbody>
										</table>
									</td>
								</tr>
							</tbody>
						</table>
					`, tasks)
			default:
				return ""
			}
		},
	}
}

func (d *DST) Step(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response, err error) (*Model, error) {
	if err != nil {
		var error *t_api.Error
		if !errors.As(err, &error) {
			return model, fmt.Errorf("unexpected error '%v'", err)
		}

		switch error.Code() {
		case t_api.StatusAPISubmissionQueueFull:
			return model, nil
		case t_api.StatusAIOSubmissionQueueFull:
			return model, nil
		case t_api.StatusSchedulerQueueFull:
			return model, nil
		default:
			return model, fmt.Errorf("unexpected resonate error '%v'", error)
		}
	}

	if req.Kind != res.Kind {
		return model, fmt.Errorf("unexpected response kind '%d' for request kind '%d'", res.Kind, req.Kind)
	}

	return d.validator.Validate(model, reqTime, resTime, req, res)
}

func (d *DST) Time(t int64) int64 {
	return t * d.config.TimeElapsedPerTick
}

func (d *DST) String() string {
	return fmt.Sprintf(
		"DST(ids=%d, idempotencyKeys=%d, headers=%d, data=%d, tags=%d, backchannel=%d)",
		d.config.Ids,
		d.config.IdempotencyKeys,
		d.config.Headers,
		d.config.Data,
		d.config.Tags,
		cap(d.config.Backchannel),
	)
}

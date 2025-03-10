package dst

import (
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sort"
	"strconv"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/task"
)

type DST struct {
	config      *Config
	generator   *Generator
	validator   *Validator
	bcValidator *BcValidator
	partitions  [][]porcupine.Operation // set by Partition in the porcupine model
}

type Config struct {
	Ticks              int64
	Timeout            time.Duration
	VisualizationPath  string
	Verbose            bool
	PrintOps           bool
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

type Partition int

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

type BcKind int

const (
	Task BcKind = iota
	Notify
)

type Backchannel struct {
	Task    *task.Task
	Promise *promise.Promise
}

func New(r *rand.Rand, config *Config) *DST {
	return &DST{
		config:      config,
		generator:   NewGenerator(r, config),
		validator:   NewValidator(r, config),
		bcValidator: NewBcValidator(r, config),
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
	d.Add(t_api.CreatePromise, d.generator.GenerateCreatePromise, d.validator.ValidateCreatePromise)
	d.Add(t_api.CreatePromiseAndTask, d.generator.GenerateCreatePromiseAndTask, d.validator.ValidateCreatePromiseAndTask)
	d.Add(t_api.CompletePromise, d.generator.GenerateCompletePromise, d.validator.ValidateCompletePromise)

	// callbacks
	d.Add(t_api.CreateCallback, d.generator.GenerateCreateCallback, d.validator.ValidateCreateCallback)

	// subscription
	d.Add(t_api.CreateSubscription, d.generator.GenerateCreateSubscription, d.validator.ValidateCreateSubscription)

	// schedules
	d.Add(t_api.ReadSchedule, d.generator.GenerateReadSchedule, d.validator.ValidateReadSchedule)
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

	// backchannel validators
	d.bcValidator.AddBcValidator(ValidateTasksWithSameRootPromiseId)
	d.bcValidator.AddBcValidator(ValidateNotify)

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

			if req.Tags == nil {
				req.Tags = make(map[string]string)
			}
			req.Tags["id"] = id
			req.Tags["name"] = req.Kind.String()

			api.EnqueueSQE(&bus.SQE[t_api.Request, t_api.Response]{
				Submission: req,
				Callback: func(res *t_api.Response, err error) {
					resTime := d.Time(t)
					if reqTime != resTime {
						resTime = resTime - 1 // subtract 1 to ensure tick timeframes don't overlap
					}

					if d.config.PrintOps {
						// log
						slog.Info("DST", "t", fmt.Sprintf("%d|%d", reqTime, resTime), "id", id, "req", req, "res", res, "err", err)
					}

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
					j++
				},
			})
			i++
		}

		system.Tick(time)

		// now read from the backchannel
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

				// The ProcessId is always the taskId, which means each task
				// is always claimed by a different process, which means
				// that when heartbeating there will always be a single task at
				// most when heartbeating

				// add claim req to generator
				d.generator.AddRequest(&t_api.Request{
					Kind: t_api.ClaimTask,
					Tags: map[string]string{"partitionId": obj.RootPromiseId},
					ClaimTask: &t_api.ClaimTaskRequest{
						Id:        obj.Id,
						Counter:   counter,
						ProcessId: obj.Id,
						Ttl:       RangeIntn(r, 1000, 5000),
					},
				})
			case *promise.Promise:
				bc = &Backchannel{
					Promise: obj,
				}
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
		slog.Error("DST is non linearizable, run with -v flag for more information", "v", d.config.Verbose)
		if d.config.Verbose {
			d.logPossibleError(history)
		}
	case porcupine.Unknown:
		slog.Error("DST timed out before linearizability could be determined")
	}

	return result == porcupine.Ok
}

func (d *DST) logPossibleError(history porcupine.LinearizationInfo) {
	// Whats is printed here and whats is visualized in the dst.html diagram might not match.
	// this is a best effort to preserve the possible validation that failed.
	fmt.Println("====== Possible errors ======")

	linearizationsPartitions := history.PartialLinearizationsOperations()

	// check each parition individually
	// partitions are in the order they were given to porcupine
	for i, partiton := range d.partitions {
		util.Assert(len(linearizationsPartitions[i]) > 0, "partition must have at least one linearization")

		// take the first (and we assumee, by empiric evidence, only linearization)
		linearization := linearizationsPartitions[i][0]

		// if the linearization includes all the operations in the partiton all good
		if len(partiton) == len(linearization) {
			continue
		}

		op := nextFailure(linearization, partiton)
		d.logError(linearization, op)
	}
}

func (d *DST) logError(partialLinearization []porcupine.Operation, lastOp porcupine.Operation) {
	// create a new model
	model := NewModel()

	// re feed operations through model
	for _, op := range partialLinearization {
		req := op.Input.(*Req)
		res := op.Output.(*Res)

		var err error

		// step through the model (again)
		if req.kind == Op {
			model, err = d.Step(model, req.time, res.time, req.req, res.res, res.err)
		} else {
			model, err = d.BcStep(model, req.time, res.time, req)
		}
		util.Assert(err == nil, "Only the last operation must result in error")
	}

	req := lastOp.Input.(*Req)
	res := lastOp.Output.(*Res)
	var err error
	if req.kind == Op {
		_, err = d.Step(model, req.time, res.time, req.req, res.res, res.err)
		fmt.Printf("Op(id=%s, t=%d|%d), req=%v, res=%v\n", req.req.Tags["id"], req.time, res.time, req.req, res.res)
	} else {
		_, err = d.BcStep(model, req.time, res.time, req)
		var obj any
		if req.bc.Task != nil {
			obj = req.bc.Task
		} else if req.bc.Promise != nil {
			obj = req.bc.Promise
		}
		fmt.Printf("Op(id=backchannel, t=%d|%d), %v\n", req.time, res.time, obj)
	}

	fmt.Printf("err=%v\n\n", err)
}

func (d *DST) Model() porcupine.Model {
	return porcupine.Model{
		Init: func() interface{} {
			return NewModel()
		},
		Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
			partitions := make(map[string][]porcupine.Operation)

			for _, op := range history {
				req := op.Input.(*Req)
				partitionKey := partition(req)
				partitions[partitionKey] = append(partitions[partitionKey], op)
			}

			// Get sorted keys to iterate over the partitions in a deterministic way
			keys := make([]string, 0, len(partitions))
			for k := range partitions {
				keys = append(keys, k)
			}
			sort.Strings(keys)

			var result [][]porcupine.Operation
			for _, key := range keys {
				result = append(result, partitions[key])
			}

			d.partitions = result
			return result
		},
		Step: func(state, input, output interface{}) (bool, interface{}) {
			model := state.(*Model)
			req := input.(*Req)
			res := output.(*Res)

			util.Assert(req.kind == res.kind, "kinds must match")

			switch req.kind {
			case Op:
				updatedModel, err := d.Step(model, req.time, res.time, req.req, res.res, res.err)
				if err != nil {
					return false, model
				}
				return true, updatedModel
			case Bc:
				updatedModel, err := d.BcStep(model, req.time, res.time, req)
				if err != nil {
					return false, model
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
				if req.bc.Task != nil {
					return fmt.Sprintf("Backchannel | %s", req.bc.Task)
				} else if req.bc.Promise != nil {
					return fmt.Sprintf("Backchannel | %s", req.bc.Promise)
				} else {
					return "Backchannel | unknown(possible error)"
				}
			default:
				panic(fmt.Sprintf("unknown request kind: %d", req.kind))
			}
		},
		DescribeState: func(state interface{}) string {
			model := state.(*Model)

			switch {
			case len(*model.promises) > 0 || len(*model.callbacks) > 0 || len(*model.tasks) > 0:
				var promises string
				for _, p := range *model.promises {
					var completedOn string
					if p.value.CompletedOn == nil {
						completedOn = "--"
					} else {
						completedOn = fmt.Sprintf("%d", *p.value.CompletedOn)
					}
					promises = promises + fmt.Sprintf(`
					<tr>
						<td align="right">%s</td>
						<td>%s</td>
						<td align="right">%s</td>
						<td align="right">%s</td>
						<td align="right">%d</td>
						<td align="right">%s</td>
					</tr>
				`, p.value.Id, p.value.State, p.value.IdempotencyKeyForCreate, p.value.IdempotencyKeyForComplete, p.value.Timeout, completedOn)
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

				var tasks string
				for _, t := range *model.tasks {
					tasks = tasks + fmt.Sprintf(`
					<tr>
						<td align="right">%s</td>
						<td align="right">%s</td>
						<td align="right">%s</td>
						<td align="right">%d</td>
						<td align="right">%d</td>
						<td align="right">%d</td>
					</tr>
				`, t.value.Id, t.value.State, t.value.RootPromiseId, t.value.ExpiresAt, t.value.Timeout, *t.value.CreatedOn)
				}
				return fmt.Sprintf(`
					<table border="0" cellspacing="0" cellpadding="5" style="background-color: white;">
					  <thead>
					    <tr>
					      <td><b>Promises</b></td>
					      <td><b>Tasks</b></td>
					    </tr>
					  </thead>
					  <tbody>
					    <!-- First Row: Promises & Tasks -->
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
					              <td><b>completedOn</b></td>
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
					              <td><b>state</b></td>
					              <td><b>rootPromiseId</b></td>
					              <td><b>expiresAt</b></td>
					              <td><b>timeout</b></td>
					              <td><b>createdOn</b></td>
					            </tr>
					          </thead>
					          <tbody>
					            %s
					          </tbody>
					        </table>
					      </td>
					    </tr>
					    <!-- Second Row: Callbacks -->
					    <tr>
					      <td colspan="2" valign="top">
					        <table border="1" cellspacing="0" cellpadding="5" style="margin-top: 10px;">
					          <thead>
					            <tr>
					              <td colspan="2"><b>Callbacks</b></td>
					            </tr>
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
				`, promises, tasks, callbacks)
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
					<table border="0" cellspacing="0" cellpadding="5" style="background-color: white;">
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
					<table border="0" cellspacing="0" cellpadding="5" style="background-color: white;">
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

func (d *DST) BcStep(model *Model, reqTime int64, resTime int64, req *Req) (*Model, error) {
	util.Assert(req.kind == Bc, "Backchannel step can only be taken if req is of kind Bc")
	if req.bc.Task == nil && req.bc.Promise == nil {
		return model, nil
	}

	return d.bcValidator.Validate(model, reqTime, resTime, req)
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

// Helper functions

func partition(req *Req) string {
	switch req.kind {
	case Op:
		partition, exists := req.req.Tags["partitionId"]
		if !exists {
			panic(fmt.Sprintf("Missing partitionId for request %v", req.req))
		}
		return partition
	case Bc:
		if req.bc.Task != nil {
			return req.bc.Task.RootPromiseId
		} else if req.bc.Promise != nil {
			return req.bc.Promise.Id
		} else {
			panic("unknown backchannel type")
		}
	default:
		panic(fmt.Sprintf("unknown request kind: %d", req.kind))
	}
}

// Find the first Operation if any that is not part of a partial linearization
// by comparing our partition with the linearization
func nextFailure(linearizationOps []porcupine.Operation, partitionOps []porcupine.Operation) porcupine.Operation {
	// convert to map for quick lookup
	linearizableMap := map[*Req]bool{}
	for _, op := range linearizationOps {
		req := op.Input.(*Req)
		linearizableMap[req] = true
	}

	for _, op := range partitionOps {
		req := op.Input.(*Req)
		// if req is part of the linearizable path, skip
		if _, ok := linearizableMap[req]; ok {
			continue
		}

		// ops are ordered by time, so the first op is not part of the
		// linearizable path should break the model
		return op
	}

	util.Assert(false, "There must be an operation not included in the linearization")
	return porcupine.Operation{}
}

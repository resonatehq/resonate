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
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
)

type DST struct {
	config    *Config
	generator *Generator
	validator *Validator
}

type Config struct {
	Ticks              int64
	TimeElapsedPerTick int64
	ReqsPerTick        func() int
	Ids                int
	IdempotencyKeys    int
	Headers            int
	Data               int
	Tags               int
}

type Req struct {
	time int64
	req  *t_api.Request
}

type Res struct {
	time int64
	res  *t_api.Response
	err  error
}

func New(r *rand.Rand, config *Config) *DST {
	return &DST{
		config:    config,
		generator: NewGenerator(r, config),
		validator: NewValidator(),
	}
}

func (d *DST) Add(kind t_api.Kind, generator RequestGenerator, validator ResponseValidator) {
	d.generator.AddRequest(generator)
	d.validator.AddResponse(kind, validator)
}

func (d *DST) Run(r *rand.Rand, api api.API, aio aio.AIO, system *system.System) bool {
	// promises
	d.Add(t_api.ReadPromise, d.generator.GenerateReadPromise, d.validator.ValidateReadPromise)
	d.Add(t_api.SearchPromises, d.generator.GenerateSearchPromises, d.validator.ValidateSearchPromises)
	d.Add(t_api.CreatePromise, d.generator.GenerateCreatePromise, d.validator.ValidateCreatePromise)
	d.Add(t_api.CompletePromise, d.generator.GenerateCompletePromise, d.validator.ValidateCompletePromise)

	// schedules
	d.Add(t_api.ReadSchedule, d.generator.GenerateReadSchedule, d.validator.ValidateReadSchedule)
	d.Add(t_api.SearchSchedules, d.generator.GenerateSearchSchedules, d.validator.ValidateSearchSchedules)
	d.Add(t_api.CreateSchedule, d.generator.GenerateCreateSchedule, d.validator.ValidateCreateSchedule)
	d.Add(t_api.DeleteSchedule, d.generator.GenerateDeleteSchedule, d.validator.ValidateDeleteSchedule)

	// locks
	d.Add(t_api.AcquireLock, d.generator.GenerateAcquireLock, d.validator.ValidateAcquireLock)
	d.Add(t_api.ReleaseLock, d.generator.GenerateReleaseLock, d.validator.ValidateReleaseLock)
	d.Add(t_api.HeartbeatLocks, d.generator.GenerateHeartbeatLocks, d.validator.ValidateHeartbeatLocks)

	var ops []porcupine.Operation

	// run all requests through the server and collect responses
	var i, j int64
	var cursors []*t_api.Request
	for t := int64(0); t < d.config.Ticks; t++ {
		time := d.Time(t)

		for _, req := range d.generator.Generate(r, time, d.config.ReqsPerTick(), &cursors) {
			tid := strconv.FormatInt(i, 10)
			req := req
			reqTime := time

			req.Tags = map[string]string{
				"request_id": tid,
				"name":       req.Kind.String(),
			}

			api.Enqueue(req, func(res *t_api.Response, err error) {
				resTime := d.Time(t)
				if reqTime != resTime {
					resTime = resTime - 1 // subtract 1 to ensure tick timeframes don't overlap
				}

				// log
				slog.Info("DST", "t", fmt.Sprintf("%d|%d", reqTime, resTime), "tid", tid, "req", req, "res", res, "err", err)

				// extract cursors for subsequent requests
				if err == nil {
					switch res.Kind {
					case t_api.SearchPromises:
						if res.SearchPromises.Cursor != nil {
							cursors = append(cursors, &t_api.Request{
								Kind:           t_api.SearchPromises,
								SearchPromises: res.SearchPromises.Cursor.Next,
							})
						}
					case t_api.SearchSchedules:
						if res.SearchSchedules.Cursor != nil {
							cursors = append(cursors, &t_api.Request{
								Kind:            t_api.SearchSchedules,
								SearchSchedules: res.SearchSchedules.Cursor.Next,
							})
						}
					}
				}

				// add operation to porcupine
				ops = append(ops, porcupine.Operation{
					ClientId: int(j % 100),
					Call:     reqTime,
					Return:   resTime,
					Input:    &Req{reqTime, req},
					Output:   &Res{resTime, res, err},
				})

				j++
			})

			i++
		}

		system.Tick(time, nil, nil)
	}

	// porcupine model
	model := porcupine.Model{
		Init: func() interface{} {
			return NewModel()
		},
		Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
			p := []porcupine.Operation{}
			s := []porcupine.Operation{}
			l := []porcupine.Operation{}

			for _, op := range history {
				switch op.Input.(*Req).req.Kind {
				case t_api.ReadPromise, t_api.SearchPromises, t_api.CreatePromise, t_api.CompletePromise:
					p = append(p, op)
				case t_api.ReadSchedule, t_api.SearchSchedules, t_api.CreateSchedule, t_api.DeleteSchedule:
					s = append(s, op)
				case t_api.AcquireLock, t_api.ReleaseLock, t_api.HeartbeatLocks:
					l = append(l, op)
				default:
					panic("unknown dst request kind")
				}
			}

			return [][]porcupine.Operation{p, s, l}
		},
		Step: func(state, input, output interface{}) (bool, interface{}) {
			model := state.(*Model)
			req := input.(*Req)
			res := output.(*Res)

			updatedModel, err := d.Step(model, req.time, res.time, req.req, res.res, res.err)
			if err != nil {
				return false, model
			}

			return true, updatedModel
		},
		Equal: func(state1, state2 interface{}) bool {
			model1 := state1.(*Model)
			model2 := state2.(*Model)

			return model1.Equals(model2)
		},
		DescribeOperation: func(input interface{}, output interface{}) string {
			req := input.(*Req)
			res := output.(*Res)

			var status t_api.ResponseStatus
			if res.res != nil {
				status = res.res.Status()
			}

			return fmt.Sprintf("%s | %s → %d", req.req.Id(), req.req, status)
		},
		DescribeState: func(state interface{}) string {
			model := state.(*Model)

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

			var schedules string
			for _, s := range *model.schedules {
				schedules = schedules + fmt.Sprintf(`
					<tr>
						<td align="right">%s</td>
						<td align="right">%s</td>
					</tr>
				`, s.value.Id, s.value.IdempotencyKey)
			}

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
							<td><b>Promises</b></td>
							<td><b>Schedules</b></td>
							<td><b>Locks</b></td>
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
											<td><b>ikey</b></td>
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
			`, promises, schedules, locks)
		},
	}

	result, history := porcupine.CheckOperationsVerbose(model, ops, 2*time.Minute)

	if err := porcupine.VisualizePath(model, history, "dst.html"); err != nil {
		slog.Error("failed to create visualization", "err", err)
		return false
	}

	return result == porcupine.Ok
}

func (d *DST) Step(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response, err error) (*Model, error) {
	if err != nil {
		var resErr *t_api.ResonateError
		if !errors.As(err, &resErr) {
			return model, fmt.Errorf("unexpected error '%v'", err)
		}

		switch resErr.Code() {
		case t_api.ErrAPISubmissionQueueFull:
			return model, nil
		case t_api.ErrAIOSubmissionQueueFull:
			return model, nil
		case t_api.ErrSchedulerQueueFull:
			return model, nil
		default:
			return model, fmt.Errorf("unexpected resonate error '%v'", resErr)
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
		"DST(ids=%d, idempotencyKeys=%d, headers=%d, data=%d, tags=%d)",
		d.config.Ids,
		d.config.IdempotencyKeys,
		d.config.Headers,
		d.config.Data,
		d.config.Tags,
	)
}

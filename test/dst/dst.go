package dst

import (
	"fmt"
	"log/slog"
	"math/rand" // nosemgrep
	"strconv"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/metadata"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
)

type DST struct {
	config *Config
}

type Config struct {
	Scenario           *Scenario
	Ticks              int64
	TimeElapsedPerTick int64
	Reqs               func() int
	Ids                int
	IdempotencyKeys    int
	Headers            int
	Data               int
	Tags               int
	Urls               int
	Retries            int
}

type Scenario struct {
	Kind           Kind
	Default        *DefaultScenario
	FaultInjection *FaultInjectionScenario
	LazyTimeout    *LazyTimeoutScenario
}

type Kind string

const (
	Default        Kind = "default"
	FaultInjection Kind = "fault"
	LazyTimeout    Kind = "lazy"
)

type DefaultScenario struct{}

type FaultInjectionScenario struct {
	P float64
}

type LazyTimeoutScenario struct{}

func New(config *Config) *DST {
	return &DST{
		config: config,
	}
}

func (d *DST) Run(r *rand.Rand, api api.API, aio aio.AIO, system *system.System, reqs []t_api.Kind) []error {
	// generator
	generator := NewGenerator(r, d.config)

	// model
	model := NewModel(d.config.Scenario)

	// add req/res
	for _, req := range reqs {
		switch req {
		// PROMISE
		case t_api.ReadPromise:
			generator.AddRequest(generator.GenerateReadPromise)
			model.AddResponse(t_api.ReadPromise, model.ValidateReadPromise)
		case t_api.SearchPromises:
			generator.AddRequest(generator.GenerateSearchPromises)
			model.AddResponse(t_api.SearchPromises, model.ValidateSearchPromises)
		case t_api.CreatePromise:
			generator.AddRequest(generator.GenerateCreatePromise)
			model.AddResponse(t_api.CreatePromise, model.ValidateCreatePromise)
		case t_api.CompletePromise:
			generator.AddRequest(generator.GenerateCompletePromise)
			model.AddResponse(t_api.CompletePromise, model.ValidateCompletePromise)

		// SCHEDULES
		case t_api.ReadSchedule:
			generator.AddRequest(generator.GenerateReadSchedule)
			model.AddResponse(t_api.ReadSchedule, model.ValidateReadSchedule)
		case t_api.SearchSchedules:
			generator.AddRequest(generator.GenerateSearchSchedules)
			model.AddResponse(t_api.SearchSchedules, model.ValidateSearchSchedules)
		case t_api.CreateSchedule:
			generator.AddRequest(generator.GenerateCreateSchedule)
			model.AddResponse(t_api.CreateSchedule, model.ValidateCreateSchedule)
		case t_api.DeleteSchedule:
			generator.AddRequest(generator.GenerateDeleteSchedule)
			model.AddResponse(t_api.DeleteSchedule, model.ValidateDeleteSchedule)

		// SUBSCRIPTION
		case t_api.ReadSubscriptions:
			generator.AddRequest(generator.GenerateReadSubscriptions)
			model.AddResponse(t_api.ReadSubscriptions, model.ValidateReadSubscriptions)
		case t_api.CreateSubscription:
			generator.AddRequest(generator.GenerateCreateSubscription)
			model.AddResponse(t_api.CreateSubscription, model.ValidateCreateSubscription)
		case t_api.DeleteSubscription:
			generator.AddRequest(generator.GenerateDeleteSubscription)
			model.AddResponse(t_api.DeleteSubscription, model.ValidateDeleteSubscription)

		// LOCK
		case t_api.AcquireLock:
			generator.AddRequest(generator.GenerateAcquireLock)
			model.AddResponse(t_api.AcquireLock, model.ValidateAcquireLock)
		case t_api.HeartbeatLocks:
			generator.AddRequest(generator.GenerateHeartbeatLocks)
			model.AddResponse(t_api.HeartbeatLocks, model.ValidateHeartbeatLocks)
		case t_api.ReleaseLock:
			generator.AddRequest(generator.GenerateReleaseLock)
			model.AddResponse(t_api.ReleaseLock, model.ValidateReleaseLock)

		// TASK
		case t_api.ClaimTask:
			generator.AddRequest(generator.GenerateClaimTask)
			model.AddResponse(t_api.ClaimTask, model.ValidateClaimTask)
		case t_api.CompleteTask:
			generator.AddRequest(generator.GenerateCompleteTask)
			model.AddResponse(t_api.CompleteTask, model.ValidateCompleteTask)
		}

	}

	// errors
	var i int64
	var errs []error

	// test loop
	for t := int64(0); t < d.config.Ticks*d.config.TimeElapsedPerTick; t += d.config.TimeElapsedPerTick {
		for _, req := range generator.Generate(r, t, d.config.Reqs(), model.cursors) {
			req := req
			reqTime := t

			metadata := metadata.New(strconv.FormatInt(i, 10))
			metadata.Tags.Set("name", req.Kind.String())
			metadata.Tags.Set("api", "dst")

			api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
				Metadata:   metadata,
				Submission: req,
				Callback: func(res *t_api.Response, err error) {
					resTime := t
					modelErr := model.Step(reqTime, resTime, req, res, err)
					if modelErr != nil {
						errs = append(errs, modelErr)
					}
					slog.Info("DST", "t", fmt.Sprintf("%d|%d", reqTime, resTime), "tid", metadata.TransactionId, "req", req, "res", res, "err", err, "ok", modelErr == nil)
				},
			})

			i++
		}

		system.Tick(t, nil)

		if len(errs) > 0 {
			break
		}
	}

	return errs
}

func (d *DST) String() string {
	return fmt.Sprintf(
		"DST(ids=%d, idempotencyKeys=%d, headers=%d, data=%d, tags=%d, urls=%d, retries=%d)",
		d.config.Ids,
		d.config.IdempotencyKeys,
		d.config.Headers,
		d.config.Data,
		d.config.Tags,
		d.config.Urls,
		d.config.Retries,
	)
}

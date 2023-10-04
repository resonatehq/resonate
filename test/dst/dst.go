package dst

import (
	"fmt"
	"log/slog"
	"math/rand"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
)

type Config struct {
	Ticks           int64
	Reqs            func() int
	Ids             int
	IdempotencyKeys int
	Headers         int
	Data            int
	Tags            int
	Urls            int
	Retries         int
}

type DST struct {
	config *Config
}

func New(config *Config) *DST {
	return &DST{
		config: config,
	}
}

func (d *DST) Run(r *rand.Rand, api api.API, aio aio.AIO, system *system.System, reqs []t_api.Kind) []error {
	// generator
	generator := NewGenerator(r, d.config)

	// model
	model := NewModel()

	// add req/res
	for _, req := range reqs {
		switch req {
		case t_api.ReadPromise:
			generator.AddRequest(generator.GenerateReadPromise)
			model.AddResponse(t_api.ReadPromise, model.ValidateReadPromise)
		case t_api.SearchPromises:
			generator.AddRequest(generator.GenerateSearchPromises)
			model.AddResponse(t_api.SearchPromises, model.ValidateSearchPromises)
		case t_api.CreatePromise:
			generator.AddRequest(generator.GenerateCreatePromise)
			model.AddResponse(t_api.CreatePromise, model.ValidatCreatePromise)
		case t_api.CancelPromise:
			generator.AddRequest(generator.GenerateCancelPromise)
			model.AddResponse(t_api.CancelPromise, model.ValidateCancelPromise)
		case t_api.ResolvePromise:
			generator.AddRequest(generator.GenerateResolvePromise)
			model.AddResponse(t_api.ResolvePromise, model.ValidateResolvePromise)
		case t_api.RejectPromise:
			generator.AddRequest(generator.GenerateRejectPromise)
			model.AddResponse(t_api.RejectPromise, model.ValidateRejectPromise)
		case t_api.ReadSubscriptions:
			generator.AddRequest(generator.GenerateReadSubscriptions)
			model.AddResponse(t_api.ReadSubscriptions, model.ValidateReadSubscriptions)
		case t_api.CreateSubscription:
			generator.AddRequest(generator.GenerateCreateSubscription)
			model.AddResponse(t_api.CreateSubscription, model.ValidateCreateSubscription)
		case t_api.DeleteSubscription:
			generator.AddRequest(generator.GenerateDeleteSubscription)
			model.AddResponse(t_api.DeleteSubscription, model.ValidateDeleteSubscription)
		}
	}

	// errors
	var errors []error

	// test loop
	for t := int64(0); t < d.config.Ticks; t++ {
		for _, req := range generator.Generate(r, t, d.config.Reqs(), model.cursors) {
			req := req
			api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
				Submission: req,
				Callback: func(res *t_api.Response, err error) {
					modelErr := model.Step(req, res, err)
					if modelErr != nil {
						errors = append(errors, modelErr)
					}

					slog.Info("DST", "t", t, "req", req, "res", res, "err", err, "ok", modelErr == nil)
				},
			})
		}

		system.Tick(t, nil)

		if len(errors) > 0 {
			break
		}
	}

	return errors
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

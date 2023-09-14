package dst

import (
	"fmt"
	"log/slog"
	"math/rand"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/types"
)

type Config struct {
	Ticks   int64
	Reqs    func() int
	Ids     int
	Ikeys   int
	Data    int
	Headers int
	Tags    int
	Urls    int
	Retries int
}

type DST struct {
	config *Config
}

func New(config *Config) *DST {
	return &DST{
		config: config,
	}
}

func (d *DST) Run(r *rand.Rand, api api.API, aio aio.AIO, system *system.System) []error {
	// generator
	generator := NewGenerator(r, d.config)
	generator.AddRequest(generator.GenerateReadPromise)
	// generator.AddRequest(generator.GenerateSearchPromises)
	generator.AddRequest(generator.GenerateCreatePromise)
	generator.AddRequest(generator.GenerateCancelPromise)
	generator.AddRequest(generator.GenerateResolvePromise)
	generator.AddRequest(generator.GenerateRejectPromise)
	generator.AddRequest(generator.GenerateReadSubscriptions)
	generator.AddRequest(generator.GenerateCreateSubscription)
	generator.AddRequest(generator.GenerateDeleteSubscription)

	// model
	model := NewModel()
	model.AddResponse(types.ReadPromise, model.ValidateReadPromise)
	model.AddResponse(types.SearchPromises, model.ValidateSearchPromises)
	model.AddResponse(types.CreatePromise, model.ValidatCreatePromise)
	model.AddResponse(types.CancelPromise, model.ValidateCancelPromise)
	model.AddResponse(types.ResolvePromise, model.ValidateResolvePromise)
	model.AddResponse(types.RejectPromise, model.ValidateRejectPromise)
	model.AddResponse(types.ReadSubscriptions, model.ValidateReadSubscriptions)
	model.AddResponse(types.CreateSubscription, model.ValidateCreateSubscription)
	model.AddResponse(types.DeleteSubscription, model.ValidateDeleteSubscription)

	// errors
	var errors []error

	// test loop
	for t := int64(0); t < d.config.Ticks; t++ {
		for _, req := range generator.Generate(r, t, d.config.Reqs()) {
			req := req
			api.Enqueue(&bus.SQE[types.Request, types.Response]{
				Submission: req,
				Callback: func(res *types.Response, err error) {
					var errMsg string
					if err != nil {
						errMsg = err.Error()
					} else {
						errMsg = "<nil>"
					}

					slog.Info("DST", "t", t, "req", req, "res", res, "err", errMsg)

					if err := model.Step(req, res, err); err != nil {
						errors = append(errors, err)
					}
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
		"DST(ids=%d, ikeys=%d, data=%d, headers=%d, tags=%d, urls=%d, retries=%d)",
		d.config.Ids,
		d.config.Ikeys,
		d.config.Data,
		d.config.Headers,
		d.config.Tags,
		d.config.Urls,
		d.config.Retries,
	)
}

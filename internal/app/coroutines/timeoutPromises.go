package coroutines

import (
	"log/slog"

	"github.com/resonatehq/gocoro"
	gocoroPromise "github.com/resonatehq/gocoro/pkg/promise"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
)

func TimeoutPromises(config *system.Config, tags map[string]string) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, any] {
	util.Assert(tags != nil, "tags must be set")

	return func(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any]) (any, error) {
		completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
			Kind: t_aio.Store,
			Tags: tags,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []*t_aio.Command{
						{
							Kind: t_aio.ReadPromises,
							ReadPromises: &t_aio.ReadPromisesCommand{
								Time:  c.Time(),
								Limit: config.PromiseBatchSize,
							},
						},
					},
				},
			},
		})

		if err != nil {
			slog.Error("failed to read promises", "err", err)
			return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
		}

		util.Assert(completion.Store != nil, "completion must not be nil")
		util.Assert(len(completion.Store.Results) == 1, "completion must have one result")

		result := completion.Store.Results[0].ReadPromises
		util.Assert(result != nil, "result must not be nil")

		awaiting := make([]gocoroPromise.Awaitable[bool], len(result.Records))

		for i, r := range result.Records {
			util.Assert(r.State == promise.Pending, "promise must be pending")
			util.Assert(r.Timeout <= c.Time(), "promise timeout must have elapsed")

			p, err := r.Promise()
			if err != nil {
				slog.Error("failed to parse promise, skipping", "err", err)
				continue
			}

			awaiting[i] = gocoro.Spawn(c, completePromise(p.Timeout, &t_api.Request{
				Kind: t_api.CompletePromise,
				Tags: tags,
				CompletePromise: &t_api.CompletePromiseRequest{
					Id:    p.Id,
					State: promise.GetTimedoutState(p),
				},
			}))
		}

		for i := 0; i < len(awaiting); i++ {
			if awaiting[i] == nil {
				continue
			}

			if _, err := gocoro.Await(c, awaiting[i]); err != nil {
				slog.Error("failed to complete promise", "err", err)
			}
		}

		return nil, nil
	}
}

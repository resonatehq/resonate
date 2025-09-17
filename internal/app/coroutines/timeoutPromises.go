package coroutines

import (
	"log/slog"

	"github.com/resonatehq/gocoro"
	gocoroPromise "github.com/resonatehq/gocoro/pkg/promise"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
)

func TimeoutPromises(config *system.Config, metadata map[string]string) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, any] {
	util.Assert(metadata != nil, "metadata must be set")

	return func(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any]) (any, error) {
		completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
			Kind: t_aio.Store,
			Tags: metadata,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []t_aio.Command{
						&t_aio.ReadPromisesCommand{
							Time:  c.Time(),
							Limit: config.PromiseBatchSize,
						},
					},
				},
			},
		})

		if err != nil {
			slog.Error("failed to read promises", "err", err)
			return nil, nil
		}

		util.Assert(completion.Store != nil, "completion must not be nil")
		util.Assert(len(completion.Store.Results) == 1, "completion must have one result")

		result := t_aio.AsQueryPromises(completion.Store.Results[0])

		awaiting := make([]gocoroPromise.Awaitable[bool], len(result.Records))
		for i, r := range result.Records {
			util.Assert(r.State == promise.Pending, "promise must be pending")
			util.Assert(r.Timeout <= c.Time(), "promise timeout must have elapsed")

			p, err := r.Promise()
			if err != nil {
				slog.Error("failed to parse promise, skipping", "err", err)
				continue
			}

			awaiting[i] = gocoro.Spawn(c, completePromise(metadata, nil, &t_aio.UpdatePromiseCommand{
				Id:             p.Id,
				State:          promise.GetTimedoutState(p),
				Value:          promise.Value{},
				IdempotencyKey: nil,
				CompletedOn:    p.Timeout,
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

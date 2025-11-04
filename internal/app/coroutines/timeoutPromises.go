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

func TimeoutPromises(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], m map[string]string) (any, error) {
	config, ok := c.Get("config").(*system.Config)
	util.Assert(ok, "coroutine must have config dependency")
	util.Assert(m != nil, "metadata must be set")

	completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
		Kind: t_aio.Store,
		Tags: m,
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

		awaiting[i] = gocoro.Spawn(c, completePromise(m, nil, &t_aio.UpdatePromiseCommand{
			Id:             p.Id,
			State:          promise.GetTimedoutState(p),
			Value:          promise.Value{},
			IdempotencyKey: nil,
			CompletedOn:    p.Timeout,
		}))
	}

	for i := range awaiting {
		if awaiting[i] == nil {
			continue
		}

		if _, err := gocoro.Await(c, awaiting[i]); err != nil {
			slog.Error("failed to complete promise", "err", err)
		}
	}

	return nil, nil
}

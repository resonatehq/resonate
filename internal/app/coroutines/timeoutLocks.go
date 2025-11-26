package coroutines

import (
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/util"
)

func TimeoutLocks(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], m map[string]string) (any, error) {
	util.Assert(m != nil, "metadata must be set")

	// try to timeout all expired locks
	completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
		Kind: t_aio.Store,
		Tags: m,
		Store: &t_aio.StoreSubmission{
			Transaction: &t_aio.Transaction{
				Commands: []t_aio.Command{
					&t_aio.TimeoutLocksCommand{
						Timeout: c.Time(),
					},
				},
			},
		},
	})
	if err != nil {
		slog.Error("failed to timeout expired locks", "err", err)
		return nil, nil
	}

	util.Assert(completion.Store != nil, "completion must not be nil")
	util.Assert(len(completion.Store.Results) == 1, "completion must have one result")
	return nil, nil
}

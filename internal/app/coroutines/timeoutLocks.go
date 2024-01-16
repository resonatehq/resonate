package coroutines

import (
	"fmt"
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/metadata"
	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/util"
)

func TimeoutLocks(t int64, config *system.Config) *Coroutine {
	metadata := metadata.New(fmt.Sprintf("tick:%d:timeoutLock", t))
	metadata.Tags.Set("name", "timeout-locks")

	return scheduler.NewCoroutine(metadata, func(c *Coroutine) {

		// Try to timeout all expired locks.
		completion, err := c.Yield(&t_aio.Submission{
			Kind: t_aio.Store,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []*t_aio.Command{
						{
							Kind: t_aio.TimeoutLocks,
							TimeoutLocks: &t_aio.TimeoutLocksCommand{
								Timeout: c.Time(),
							},
						},
					},
				},
			},
		})
		if err != nil {
			slog.Error("failed to timeout expired locks", "err", err)
			return
		}

		// up the asserts !
		util.Assert(completion.Store != nil, "completion must not be nil")
		util.Assert(len(completion.Store.Results) == 1, "completion must have three results")
	})
}

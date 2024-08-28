package coroutines

import (
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
)

func HeartbeatLocks(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	// Try to update all locks that belong to this process.
	completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
		Kind: t_aio.Store,
		Tags: r.Tags,
		Store: &t_aio.StoreSubmission{
			Transaction: &t_aio.Transaction{
				Commands: []*t_aio.Command{
					{
						Kind: t_aio.HeartbeatLocks,
						HeartbeatLocks: &t_aio.HeartbeatLocksCommand{
							ProcessId: r.HeartbeatLocks.ProcessId,
							Time:      c.Time(),
						},
					},
				},
			},
		},
	})
	if err != nil {
		slog.Error("failed to release lock", "req", r, "err", err)
		return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
	}

	util.Assert(completion.Store != nil, "completion must not be nil")
	result := completion.Store.Results[0].HeartbeatLocks

	return &t_api.Response{
		Kind: t_api.HeartbeatLocks,
		Tags: r.Tags,
		HeartbeatLocks: &t_api.HeartbeatLocksResponse{
			Status:        t_api.StatusOK,
			LocksAffected: result.RowsAffected,
		},
	}, nil
}

package coroutines

import (
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
)

func HeartbeatLocks(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	req := r.Payload.(*t_api.HeartbeatLocksRequest)

	// Try to update all locks that belong to this process.
	completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
		Kind: t_aio.Store,
		Tags: r.Metadata,
		Store: &t_aio.StoreSubmission{
			Transaction: &t_aio.Transaction{
				Commands: []t_aio.Command{
					&t_aio.HeartbeatLocksCommand{
						ProcessId: req.ProcessId,
						Time:      c.Time(),
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
	result := t_aio.AsAlterLocks(completion.Store.Results[0])

	return &t_api.Response{
		Status:   t_api.StatusOK,
		Metadata: r.Metadata,
		Payload: &t_api.HeartbeatLocksResponse{
			LocksAffected: result.RowsAffected,
		},
	}, nil
}

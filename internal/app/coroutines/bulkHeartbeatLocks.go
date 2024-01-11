package coroutines

import (
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/metadata"
	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
)

func BulkHeartbeatLocks(metadata *metadata.Metadata, req *t_api.Request, res CallBackFn) *Coroutine {
	return scheduler.NewCoroutine(metadata, func(c *Coroutine) {

		// Try to update all locks that belong to this process.
		completion, err := c.Yield(&t_aio.Submission{
			Kind: t_aio.Store,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []*t_aio.Command{
						{
							Kind: t_aio.BulkHeartbeatLocks,
							BulkHeartbeatLocks: &t_aio.BulkHeartbeatLocksCommand{
								ProcessId: req.BulkHeartbeatLocks.ProcessId,
								// todo: risk of lock expiring unexpectedly.
								Timeout: req.BulkHeartbeatLocks.Timeout,
							},
						},
					},
				},
			},
		})
		if err != nil {
			slog.Error("failed to release lock", "req", req, "err", err)
			res(nil, t_api.NewResonateError(t_api.ErrAIOStoreFailure, "failed to release lock", err))
			return
		}

		util.Assert(completion.Store != nil, "completion must not be nil")
		result := completion.Store.Results[0].BulkHeartbeatLocks

		// If rows affected is 0, the the process does not own any locks.
		if result.RowsAffected == 0 {
			res(&t_api.Response{
				Kind: t_api.BulkHeartbeatLocks,
				BulkHeartbeatLocks: &t_api.BulkHeartbeatLocksResponse{
					Status: t_api.StatusLockNotFound, // todo: correct behavior.
				},
			}, nil)
			return
		}

		// If rows affected is greater than 0, then the lock's leases were renewed for the processId.
		res(&t_api.Response{
			Kind: t_api.BulkHeartbeatLocks,
			BulkHeartbeatLocks: &t_api.BulkHeartbeatLocksResponse{
				Status: t_api.StatusOK,
			},
		}, nil)
	})
}

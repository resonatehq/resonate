package coroutines

import (
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/metadata"
	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
)

func ReleaseLock(metadata *metadata.Metadata, req *t_api.Request, res CallBackFn) *Coroutine {
	return scheduler.NewCoroutine(metadata, func(c *Coroutine) {

		// Try to release lock.
		completion, err := c.Yield(&t_aio.Submission{
			Kind: t_aio.Store,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []*t_aio.Command{
						{
							Kind: t_aio.ReleaseLock,
							ReleaseLock: &t_aio.ReleaseLockCommand{
								ResourceId:  req.ReleaseLock.ResourceId,
								ExecutionId: req.ReleaseLock.ExecutionId,
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
		result := completion.Store.Results[0].ReleaseLock
		util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

		// ReleaseLock only competes with the timeout release lock coroutine when the client
		// tries to unlock a lock that expired. In this case, the timeout release lock coroutine
		// Delete operations are already idempotent.
		// If rows affected is 0, then lock was already released or was never acquired by that executionId.
		if result.RowsAffected == 0 {
			res(&t_api.Response{
				Kind: t_api.ReleaseLock,
				ReleaseLock: &t_api.ReleaseLockResponse{
					Status: t_api.StatusLockNotFound,
				},
			}, nil)
			return
		}

		// If rows affected is 1, then lock is released.
		res(&t_api.Response{
			Kind: t_api.ReleaseLock,
			ReleaseLock: &t_api.ReleaseLockResponse{
				Status: t_api.StatusNoContent,
			},
		}, nil)
	})
}

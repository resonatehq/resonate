package coroutines

import (
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/metadata"
	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/lock"
)

func AcquireLock(metadata *metadata.Metadata, req *t_api.Request, res CallBackFn) *Coroutine {

	return scheduler.NewCoroutine(metadata, func(c *Coroutine) {

		// Try to acquire lock (upsert). Update lock if already acquired by the same executionId.
		completion, err := c.Yield(&t_aio.Submission{
			Kind: t_aio.Store,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []*t_aio.Command{
						{
							Kind: t_aio.AcquireLock,
							AcquireLock: &t_aio.AcquireLockCommand{
								ResourceId:  req.AcquireLock.ResourceId,
								ProcessId:   req.AcquireLock.ProcessId,
								ExecutionId: req.AcquireLock.ExecutionId,
								Timeout:     c.Time() + (req.AcquireLock.ExpiryInSeconds * 1000), // from s to ms
							},
						},
					},
				},
			},
		})
		if err != nil {
			slog.Error("failed to acquire lock", "req", req, "err", err)
			res(nil, t_api.NewResonateError(t_api.ErrAIOStoreFailure, "failed to acquire lock", err))
			return
		}

		util.Assert(completion.Store != nil, "completion must not be nil")
		result := completion.Store.Results[0].AcquireLock
		util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

		// If an attempt to acquire a lock resulted in 0 affected rows, then there is only one possibility:
		// 1. lock is already acquired by another process.
		if result.RowsAffected == 0 {
			res(&t_api.Response{
				Kind: t_api.AcquireLock,
				AcquireLock: &t_api.AcquireLockResponse{
					Status: t_api.StatusLockAlreadyAcquired,
				},
			}, nil)
			return
		}

		// If rows affected is 1, then lock is acquired.
		res(&t_api.Response{
			Kind: t_api.AcquireLock,
			AcquireLock: &t_api.AcquireLockResponse{
				Status: t_api.StatusCreated,
				Lock: &lock.Lock{
					ResourceId:      req.AcquireLock.ResourceId,
					ProcessId:       req.AcquireLock.ProcessId,
					ExecutionId:     req.AcquireLock.ExecutionId,
					ExpiryInSeconds: req.AcquireLock.ExpiryInSeconds,
				},
			},
		}, nil)
	})
}

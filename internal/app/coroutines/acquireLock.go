package coroutines

import (
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/lock"
)

func AcquireLock(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	expiresAt := c.Time() + r.AcquireLock.ExpiryInMilliseconds

	// Try to acquire lock, update lock if already acquired by the same execution id
	completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
		Kind: t_aio.Store,
		Tags: r.Tags,
		Store: &t_aio.StoreSubmission{
			Transaction: &t_aio.Transaction{
				Commands: []*t_aio.Command{
					{
						Kind: t_aio.AcquireLock,
						AcquireLock: &t_aio.AcquireLockCommand{
							ResourceId:           r.AcquireLock.ResourceId,
							ProcessId:            r.AcquireLock.ProcessId,
							ExecutionId:          r.AcquireLock.ExecutionId,
							ExpiryInMilliseconds: r.AcquireLock.ExpiryInMilliseconds,
							Timeout:              expiresAt,
						},
					},
				},
			},
		},
	})
	if err != nil {
		slog.Error("failed to acquire lock", "req", r, "err", err)
		return nil, t_api.NewResonateError(t_api.ErrAIOStoreFailure, "failed to acquire lock", err)
	}

	util.Assert(completion.Store != nil, "completion must not be nil")
	result := completion.Store.Results[0].AcquireLock
	util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

	var res *t_api.Response

	// If an attempt to acquire a lock results in 0 rows affected, then the lock is already acquired
	if result.RowsAffected == 0 {
		res = &t_api.Response{
			Kind: t_api.AcquireLock,
			AcquireLock: &t_api.AcquireLockResponse{
				Status: t_api.StatusLockAlreadyAcquired,
			},
		}
	} else {
		res = &t_api.Response{
			Kind: t_api.AcquireLock,
			Tags: r.Tags,
			AcquireLock: &t_api.AcquireLockResponse{
				Status: t_api.StatusCreated,
				Lock: &lock.Lock{
					ResourceId:           r.AcquireLock.ResourceId,
					ProcessId:            r.AcquireLock.ProcessId,
					ExecutionId:          r.AcquireLock.ExecutionId,
					ExpiryInMilliseconds: r.AcquireLock.ExpiryInMilliseconds,
					ExpiresAt:            expiresAt,
				},
			},
		}
	}

	util.Assert(res != nil, "response must not be nil")
	return res, nil
}

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
	req := r.Payload.(*t_api.AcquireLockRequest)

	expiresAt := c.Time() + req.Ttl

	// Try to acquire lock, update lock if already acquired by the same execution id
	completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
		Kind: t_aio.Store,
		Tags: r.Metadata,
		Store: &t_aio.StoreSubmission{
			Transaction: &t_aio.Transaction{
				Commands: []*t_aio.Command{
					{
						Kind: t_aio.AcquireLock,
						AcquireLock: &t_aio.AcquireLockCommand{
							ResourceId:  req.ResourceId,
							ExecutionId: req.ExecutionId,
							ProcessId:   req.ProcessId,
							Ttl:         req.Ttl,
							ExpiresAt:   expiresAt,
						},
					},
				},
			},
		},
	})
	if err != nil {
		slog.Error("failed to acquire lock", "req", r, "err", err)
		return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
	}

	util.Assert(completion.Store != nil, "completion must not be nil")
	result := completion.Store.Results[0].AcquireLock
	util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

	var res *t_api.Response

	// If an attempt to acquire a lock results in 0 rows affected, then the lock is already acquired
	if result.RowsAffected == 0 {
		res = &t_api.Response{
			Status:  t_api.StatusLockAlreadyAcquired,
			Payload: &t_api.AcquireLockResponse{},
		}
	} else {
		res = &t_api.Response{
			Status:   t_api.StatusCreated,
			Metadata: r.Metadata,
			Payload: &t_api.AcquireLockResponse{
				Lock: &lock.Lock{
					ResourceId:  req.ResourceId,
					ExecutionId: req.ExecutionId,
					ProcessId:   req.ProcessId,
					Ttl:         req.Ttl,
					ExpiresAt:   expiresAt,
				},
			},
		}
	}

	util.Assert(res != nil, "response must not be nil")
	return res, nil
}

package coroutines

import (
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
)

func ReleaseLock(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	req := r.Payload.(*t_api.ReleaseLockRequest)

	// Try to release lock.
	completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
		Kind: t_aio.Store,
		Tags: r.Metadata,
		Store: &t_aio.StoreSubmission{
			Transaction: &t_aio.Transaction{
				Commands: []*t_aio.Command{
					{
						Kind: t_aio.ReleaseLock,
						ReleaseLock: &t_aio.ReleaseLockCommand{
							ResourceId:  req.ResourceId,
							ExecutionId: req.ExecutionId,
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
	result := completion.Store.Results[0].ReleaseLock
	util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

	var res *t_api.Response

	if result.RowsAffected == 0 {
		// ReleaseLock only competes with the timeout release lock coroutine when the client
		// tries to unlock a lock that expired. In this case, the timeout release lock coroutine
		// Delete operations are already idempotent.
		// If rows affected is 0, then lock was already released or was never acquired by that executionId.
		res = &t_api.Response{
			Status:   t_api.StatusLockNotFound,
			Metadata: r.Metadata,
			Payload:  &t_api.ReleaseLockResponse{},
		}
	} else {
		// If rows affected is 1, then lock is released.
		res = &t_api.Response{
			Status:   t_api.StatusNoContent,
			Metadata: r.Metadata,
			Payload:  &t_api.ReleaseLockResponse{},
		}
	}

	util.Assert(res != nil, "response must not be nil")
	return res, nil
}

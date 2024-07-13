package coroutines

import (
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
)

func DeleteSubscription(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
		Kind: t_aio.Store,
		Tags: r.Tags,
		Store: &t_aio.StoreSubmission{
			Transaction: &t_aio.Transaction{
				Commands: []*t_aio.Command{
					{
						Kind: t_aio.DeleteSubscription,
						DeleteSubscription: &t_aio.DeleteSubscriptionCommand{
							Id:        r.DeleteSubscription.Id,
							PromiseId: r.DeleteSubscription.PromiseId,
						},
					},
				},
			},
		},
	})

	if err != nil {
		slog.Error("failed to delete subscription", "req", r, "err", err)
		return nil, t_api.NewResonateError(t_api.ErrAIOStoreFailure, "failed to delete subscription", err)
	}

	util.Assert(completion.Store != nil, "completion must not be nil")

	result := completion.Store.Results[0].DeleteSubscription
	util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

	var status t_api.ResponseStatus

	if result.RowsAffected == 1 {
		status = t_api.StatusNoContent
	} else {
		status = t_api.StatusSubscriptionNotFound
	}

	return &t_api.Response{
		Kind: t_api.DeleteSubscription,
		Tags: r.Tags,
		DeleteSubscription: &t_api.DeleteSubscriptionResponse{
			Status: status,
		},
	}, nil
}

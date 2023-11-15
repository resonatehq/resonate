package coroutines

import (
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/metadata"
	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
)

func DeleteSubscription(metadata *metadata.Metadata, req *t_api.Request, res func(*t_api.Response, error)) *scheduler.Coroutine[*t_aio.Completion, *t_aio.Submission] {
	return scheduler.NewCoroutine(metadata, func(c *scheduler.Coroutine[*t_aio.Completion, *t_aio.Submission]) {
		completion, err := c.Yield(&t_aio.Submission{
			Kind: t_aio.Store,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []*t_aio.Command{
						{
							Kind: t_aio.DeleteSubscription,
							DeleteSubscription: &t_aio.DeleteSubscriptionCommand{
								Id:        req.DeleteSubscription.Id,
								PromiseId: req.DeleteSubscription.PromiseId,
							},
						},
					},
				},
			},
		})

		if err != nil {
			slog.Error("failed to delete subscription", "req", req, "err", err)
			res(nil, t_api.NewResonateError(t_api.ErrFailedToDeleteSubscription, err.Error()))
			return
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

		res(&t_api.Response{
			Kind: t_api.DeleteSubscription,
			DeleteSubscription: &t_api.DeleteSubscriptionResponse{
				Status: status,
			},
		}, nil)
	})
}

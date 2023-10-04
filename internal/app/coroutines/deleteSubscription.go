package coroutines

import (
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
)

func DeleteSubscription(t int64, req *t_api.Request, res func(int64, *t_api.Response, error)) *scheduler.Coroutine {
	return scheduler.NewCoroutine("DeleteSubscription", func(s *scheduler.Scheduler, c *scheduler.Coroutine) {
		submission := &t_aio.Submission{
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
		}

		c.Yield(submission, func(t int64, completion *t_aio.Completion, err error) {
			if err != nil {
				slog.Error("failed to delete subscription", "req", req, "err", err)
				res(t, nil, err)
				return
			}

			util.Assert(completion.Store != nil, "completion must not be nil")

			result := completion.Store.Results[0].DeleteSubscription
			util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

			var status t_api.ResponseStatus

			if result.RowsAffected == 1 {
				status = t_api.ResponseNoContent
			} else {
				status = t_api.ResponseNotFound
			}

			res(t, &t_api.Response{
				Kind: t_api.DeleteSubscription,
				DeleteSubscription: &t_api.DeleteSubscriptionResponse{
					Status: status,
				},
			}, nil)
		})
	})
}

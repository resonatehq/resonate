package coroutines

import (
	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/resonatehq/resonate/internal/util"
)

func DeleteSubscription(t int64, req *types.Request, res func(*types.Response, error)) *scheduler.Coroutine {
	return scheduler.NewCoroutine(func(s *scheduler.Scheduler, c *scheduler.Coroutine) {
		submission := &types.Submission{
			Kind: types.Store,
			Store: &types.StoreSubmission{
				Transaction: &types.Transaction{
					Commands: []*types.Command{
						{
							Kind: types.StoreDeleteSubscription,
							DeleteSubscription: &types.DeleteSubscriptionCommand{
								Id: req.DeleteSubscription.Id,
							},
						},
					},
				},
			},
		}

		c.Yield(submission, func(completion *types.Completion, err error) {
			if err != nil {
				res(nil, err)
				return
			}

			util.Assert(completion.Store != nil, "completion must not be nil")

			result := completion.Store.Results[0].DeleteSubscription
			util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

			var status types.ResponseStatus

			if result.RowsAffected == 1 {
				status = types.ResponseNoContent
			} else {
				status = types.ResponseNotFound
			}

			res(&types.Response{
				Kind: types.DeleteSubscription,
				DeleteSubscription: &types.DeleteSubscriptionResponse{
					Status: status,
				},
			}, nil)
		})
	})
}

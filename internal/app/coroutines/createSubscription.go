package coroutines

import (
	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/subscription"
)

func CreateSubscription(t int64, req *types.Request, res func(*types.Response, error)) *scheduler.Coroutine {
	return scheduler.NewCoroutine(func(s *scheduler.Scheduler, c *scheduler.Coroutine) {
		submission := &types.Submission{
			Kind: types.Store,
			Store: &types.StoreSubmission{
				Transaction: &types.Transaction{
					Commands: []*types.Command{
						{
							Kind: types.StoreCreateSubscription,
							CreateSubscription: &types.CreateSubscriptionCommand{
								PromiseId: req.CreateSubscription.PromiseId,
								Url:       req.CreateSubscription.Url,
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

			result := completion.Store.Results[0].CreateSubscription
			util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

			if result.RowsAffected == 1 {
				res(&types.Response{
					Kind: types.CreateSubscription,
					CreateSubscription: &types.CreateSubscriptionResponse{
						Status: types.ResponseCreated,
						Subscription: &subscription.Subscription{
							Id:  result.LastInsertId,
							Url: req.CreateSubscription.Url,
						},
					},
				}, nil)
			} else {
				res(&types.Response{
					Kind: types.CreateSubscription,
					CreateSubscription: &types.CreateSubscriptionResponse{
						Status: types.ResponseConflict,
					},
				}, nil)
			}
		})
	})
}

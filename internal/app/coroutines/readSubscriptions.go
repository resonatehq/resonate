package coroutines

import (
	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/subscription"
)

func ReadSubscriptions(t int64, req *types.Request, res func(*types.Response, error)) *scheduler.Coroutine {
	return scheduler.NewCoroutine(func(s *scheduler.Scheduler, c *scheduler.Coroutine) {
		submission := &types.Submission{
			Kind: types.Store,
			Store: &types.StoreSubmission{
				Transaction: &types.Transaction{
					Commands: []*types.Command{
						{
							Kind: types.StoreReadSubscriptions,
							ReadSubscriptions: &types.ReadSubscriptionsCommand{
								PromiseIds: []string{req.ReadSubscriptions.PromiseId},
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

			records := completion.Store.Results[0].ReadSubscriptions.Records
			subscriptions := []*subscription.Subscription{}

			for _, records := range records {
				subscriptions = append(subscriptions, records.Subscription())
			}

			res(&types.Response{
				Kind: types.ReadSubscriptions,
				ReadSubscriptions: &types.ReadSubscriptionsResponse{
					Status:        types.ResponseOK,
					Subscriptions: subscriptions,
				},
			}, nil)
		})
	})
}

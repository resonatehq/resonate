package coroutines

import (
	"fmt"
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/subscription"
)

func CreateSubscription(t int64, req *types.Request, res func(*types.Response, error)) *scheduler.Coroutine {
	return scheduler.NewCoroutine(fmt.Sprintf("CreateSubscription(id=%s, promiseId=%s)", req.CreateSubscription.Id, req.CreateSubscription.PromiseId), "CreateSubscription", func(s *scheduler.Scheduler, c *scheduler.Coroutine) {
		// default retry policy
		if req.CreateSubscription.RetryPolicy == nil {
			req.CreateSubscription.RetryPolicy = &subscription.RetryPolicy{
				Delay:    30,
				Attempts: 3,
			}
		}

		submission := &types.Submission{
			Kind: types.Store,
			Store: &types.StoreSubmission{
				Transaction: &types.Transaction{
					Commands: []*types.Command{
						{
							Kind: types.StoreCreateSubscription,
							CreateSubscription: &types.CreateSubscriptionCommand{
								Id:          req.CreateSubscription.Id,
								PromiseId:   req.CreateSubscription.PromiseId,
								Url:         req.CreateSubscription.Url,
								RetryPolicy: req.CreateSubscription.RetryPolicy,
								CreatedOn:   t,
							},
						},
					},
				},
			},
		}

		c.Yield(submission, func(completion *types.Completion, err error) {
			if err != nil {
				slog.Error("failed to create subscription", "req", req, "err", err)
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
							Id:          req.CreateSubscription.Id,
							PromiseId:   req.CreateSubscription.PromiseId,
							Url:         req.CreateSubscription.Url,
							RetryPolicy: req.CreateSubscription.RetryPolicy,
						},
					},
				}, nil)
			} else {
				submission := &types.Submission{
					Kind: types.Store,
					Store: &types.StoreSubmission{
						Transaction: &types.Transaction{
							Commands: []*types.Command{
								{
									Kind: types.StoreReadSubscription,
									ReadSubscription: &types.ReadSubscriptionCommand{
										Id:        req.CreateSubscription.Id,
										PromiseId: req.CreateSubscription.PromiseId,
									},
								},
							},
						},
					},
				}

				c.Yield(submission, func(completion *types.Completion, err error) {
					if err != nil {
						slog.Error("failed to read subscription", "req", req, "err", err)
						res(nil, err)
						return
					}

					util.Assert(completion.Store != nil, "completion must not be nil")

					result := completion.Store.Results[0].ReadSubscription
					util.Assert(result.RowsReturned == 0 || result.RowsReturned == 1, "result must return 0 or 1 rows")

					if result.RowsReturned == 1 {
						subscription, err := result.Records[0].Subscription()
						if err != nil {
							slog.Error("failed to parse subscription record", "record", result.Records[0], "err", err)
							res(nil, err)
							return
						}

						res(&types.Response{
							Kind: types.CreateSubscription,
							CreateSubscription: &types.CreateSubscriptionResponse{
								Status:       types.ResponseOK,
								Subscription: subscription,
							},
						}, nil)
					} else {
						s.Add(CreateSubscription(t, req, res))
					}
				})
			}
		})
	})
}

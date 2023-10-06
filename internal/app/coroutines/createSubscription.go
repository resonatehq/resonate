package coroutines

import (
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/subscription"
)

func CreateSubscription(req *t_api.Request, res func(*t_api.Response, error)) *scheduler.Coroutine {
	return scheduler.NewCoroutine("CreateSubscription", func(s *scheduler.Scheduler, c *scheduler.Coroutine) {
		// default retry policy
		if req.CreateSubscription.RetryPolicy == nil {
			req.CreateSubscription.RetryPolicy = &subscription.RetryPolicy{
				Delay:    30,
				Attempts: 3,
			}
		}

		createdOn := s.Time()
		submission := &t_aio.Submission{
			Kind: t_aio.Store,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []*t_aio.Command{
						{
							Kind: t_aio.CreateSubscription,
							CreateSubscription: &t_aio.CreateSubscriptionCommand{
								Id:          req.CreateSubscription.Id,
								PromiseId:   req.CreateSubscription.PromiseId,
								Url:         req.CreateSubscription.Url,
								RetryPolicy: req.CreateSubscription.RetryPolicy,
								CreatedOn:   createdOn,
							},
						},
					},
				},
			},
		}

		c.Yield(submission, func(completion *t_aio.Completion, err error) {
			if err != nil {
				slog.Error("failed to create subscription", "req", req, "err", err)
				res(nil, err)
				return
			}

			util.Assert(completion.Store != nil, "completion must not be nil")

			result := completion.Store.Results[0].CreateSubscription
			util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

			if result.RowsAffected == 1 {
				res(&t_api.Response{
					Kind: t_api.CreateSubscription,
					CreateSubscription: &t_api.CreateSubscriptionResponse{
						Status: t_api.ResponseCreated,
						Subscription: &subscription.Subscription{
							Id:          req.CreateSubscription.Id,
							PromiseId:   req.CreateSubscription.PromiseId,
							Url:         req.CreateSubscription.Url,
							RetryPolicy: req.CreateSubscription.RetryPolicy,
							CreatedOn:   createdOn,
						},
					},
				}, nil)
			} else {
				submission := &t_aio.Submission{
					Kind: t_aio.Store,
					Store: &t_aio.StoreSubmission{
						Transaction: &t_aio.Transaction{
							Commands: []*t_aio.Command{
								{
									Kind: t_aio.ReadSubscription,
									ReadSubscription: &t_aio.ReadSubscriptionCommand{
										Id:        req.CreateSubscription.Id,
										PromiseId: req.CreateSubscription.PromiseId,
									},
								},
							},
						},
					},
				}

				c.Yield(submission, func(completion *t_aio.Completion, err error) {
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

						res(&t_api.Response{
							Kind: t_api.CreateSubscription,
							CreateSubscription: &t_api.CreateSubscriptionResponse{
								Status:       t_api.ResponseOK,
								Subscription: subscription,
							},
						}, nil)
					} else {
						s.Add(CreateSubscription(req, res))
					}
				})
			}
		})
	})
}

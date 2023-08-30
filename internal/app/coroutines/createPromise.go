package coroutines

import (
	"fmt"
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/subscription"
)

func CreatePromise(t int64, req *types.Request, res func(*types.Response, error)) *scheduler.Coroutine {
	return scheduler.NewCoroutine(fmt.Sprintf("CreatePromise(id=%s)", req.CreatePromise.Id), "CreatePromise", func(s *scheduler.Scheduler, c *scheduler.Coroutine) {
		if req.CreatePromise.Param.Headers == nil {
			req.CreatePromise.Param.Headers = map[string]string{}
		}
		if req.CreatePromise.Tags == nil {
			req.CreatePromise.Tags = map[string]string{}
		}

		submission := &types.Submission{
			Kind: types.Store,
			Store: &types.StoreSubmission{
				Transaction: &types.Transaction{
					Commands: []*types.Command{
						{
							Kind: types.StoreReadPromise,
							ReadPromise: &types.ReadPromiseCommand{
								Id: req.CreatePromise.Id,
							},
						},
					},
				},
			},
		}

		c.Yield(submission, func(completion *types.Completion, err error) {
			if err != nil {
				slog.Error("failed to read promise", "req", req, "err", err)
				res(nil, err)
				return
			}

			util.Assert(completion.Store != nil, "completion must not be nil")

			result := completion.Store.Results[0].ReadPromise
			util.Assert(result.RowsReturned == 0 || result.RowsReturned == 1, "result must return 0 or 1 rows")

			if result.RowsReturned == 0 {
				commands := []*types.Command{
					{
						Kind: types.StoreCreatePromise,
						CreatePromise: &types.CreatePromiseCommand{
							Id:        req.CreatePromise.Id,
							Param:     req.CreatePromise.Param,
							Timeout:   req.CreatePromise.Timeout,
							Tags:      req.CreatePromise.Tags,
							CreatedOn: t,
						},
					},
					{
						Kind: types.StoreCreateTimeout,
						CreateTimeout: &types.CreateTimeoutCommand{
							Id:   req.CreatePromise.Id,
							Time: req.CreatePromise.Timeout,
						},
					},
				}

				for _, s := range req.CreatePromise.Subscriptions {
					// default retry policy
					if s.RetryPolicy == nil {
						s.RetryPolicy = &subscription.RetryPolicy{
							Delay:    30,
							Attempts: 3,
						}
					}

					commands = append(commands, &types.Command{
						Kind: types.StoreCreateSubscription,
						CreateSubscription: &types.CreateSubscriptionCommand{
							PromiseId:   req.CreatePromise.Id,
							Url:         s.Url,
							RetryPolicy: s.RetryPolicy,
							CreatedOn:   t,
						},
					})
				}

				submission := &types.Submission{
					Kind: types.Store,
					Store: &types.StoreSubmission{
						Transaction: &types.Transaction{
							Commands: commands,
						},
					},
				}

				c.Yield(submission, func(completion *types.Completion, err error) {
					if err != nil {
						slog.Error("failed to update state", "req", req, "err", err)
						res(nil, err)
						return
					}

					util.Assert(completion.Store != nil, "completion must not be nil")

					result := completion.Store.Results[0].CreatePromise
					util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

					if result.RowsAffected == 1 {
						res(&types.Response{
							Kind: types.CreatePromise,
							CreatePromise: &types.CreatePromiseResponse{
								Status: types.ResponseCreated,
								Promise: &promise.Promise{
									Id:        req.CreatePromise.Id,
									State:     promise.Pending,
									Param:     req.CreatePromise.Param,
									Timeout:   req.CreatePromise.Timeout,
									Tags:      req.CreatePromise.Tags,
									CreatedOn: &t,
								},
							},
						}, nil)
					} else {
						s.Add(CreatePromise(t, req, res))
					}
				})
			} else {
				p, err := result.Records[0].Promise()
				if err != nil {
					slog.Error("failed to parse promise record", "record", result.Records[0], "err", err)
					res(nil, err)
					return
				}

				var status types.ResponseStatus
				if p.Param.Ikey.Match(req.CreatePromise.Param.Ikey) {
					status = types.ResponseOK
				} else {
					status = types.ResponseForbidden
				}

				if p.State == promise.Pending && t >= p.Timeout {
					s.Add(TimeoutPromise(t, p, CreatePromise(t, req, res), func(err error) {
						if err != nil {
							slog.Error("failed to timeout promise", "req", req, "err", err)
							res(nil, err)
							return
						}

						res(&types.Response{
							Kind: types.CreatePromise,
							CreatePromise: &types.CreatePromiseResponse{
								Status: status,
								Promise: &promise.Promise{
									Id:    p.Id,
									State: promise.Timedout,
									Param: p.Param,
									Value: promise.Value{
										Headers: map[string]string{},
										Ikey:    nil,
										Data:    nil,
									},
									Timeout:     p.Timeout,
									Tags:        p.Tags,
									CreatedOn:   p.CreatedOn,
									CompletedOn: &p.Timeout,
								},
							},
						}, nil)
					}))
				} else {
					res(&types.Response{
						Kind: types.CreatePromise,
						CreatePromise: &types.CreatePromiseResponse{
							Status:  status,
							Promise: p,
						},
					}, nil)
				}
			}
		})
	})
}

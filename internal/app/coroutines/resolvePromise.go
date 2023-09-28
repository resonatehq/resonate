package coroutines

import (
	"fmt"
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
)

func ResolvePromise(t int64, req *types.Request, res func(int64, *types.Response, error)) *scheduler.Coroutine {
	return scheduler.NewCoroutine(fmt.Sprintf("ResolvePromise(id=%s)", req.ResolvePromise.Id), "ResolvePromise", func(s *scheduler.Scheduler, c *scheduler.Coroutine) {
		if req.ResolvePromise.Value.Headers == nil {
			req.ResolvePromise.Value.Headers = map[string]string{}
		}
		if req.ResolvePromise.Value.Data == nil {
			req.ResolvePromise.Value.Data = []byte{}
		}

		submission := &types.Submission{
			Kind: types.Store,
			Store: &types.StoreSubmission{
				Transaction: &types.Transaction{
					Commands: []*types.Command{
						{
							Kind: types.StoreReadPromise,
							ReadPromise: &types.ReadPromiseCommand{
								Id: req.ResolvePromise.Id,
							},
						},
					},
				},
			},
		}

		c.Yield(submission, func(t int64, completion *types.Completion, err error) {
			if err != nil {
				slog.Error("failed to read promise or read subscriptions", "req", req, "err", err)
				res(t, nil, err)
				return
			}

			util.Assert(completion.Store != nil, "completion must not be nil")

			result := completion.Store.Results[0].ReadPromise
			util.Assert(result.RowsReturned == 0 || result.RowsReturned == 1, "result must return 0 or 1 rows")

			if result.RowsReturned == 0 {
				res(t, &types.Response{
					Kind: types.ResolvePromise,
					ResolvePromise: &types.ResolvePromiseResponse{
						Status: types.ResponseNotFound,
					},
				}, nil)
			} else {
				p, err := result.Records[0].Promise()
				if err != nil {
					slog.Error("failed to parse promise record", "record", result.Records[0], "err", err)
					res(t, nil, err)
					return
				}

				if p.State == promise.Pending {
					if t >= p.Timeout {
						s.Add(TimeoutPromise(t, p, ResolvePromise(t, req, res), func(t int64, err error) {
							if err != nil {
								slog.Error("failed to timeout promise", "req", req, "err", err)
								res(t, nil, err)
								return
							}

							res(t, &types.Response{
								Kind: types.ResolvePromise,
								ResolvePromise: &types.ResolvePromiseResponse{
									Status: types.ResponseForbidden,
									Promise: &promise.Promise{
										Id:    p.Id,
										State: promise.Timedout,
										Param: p.Param,
										Value: promise.Value{
											Headers: map[string]string{},
											Ikey:    nil,
											Data:    []byte{},
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
						submission := &types.Submission{
							Kind: types.Store,
							Store: &types.StoreSubmission{
								Transaction: &types.Transaction{
									Commands: []*types.Command{
										{
											Kind: types.StoreUpdatePromise,
											UpdatePromise: &types.UpdatePromiseCommand{
												Id:          req.ResolvePromise.Id,
												State:       promise.Resolved,
												Value:       req.ResolvePromise.Value,
												CompletedOn: t,
											},
										},
										{
											Kind: types.StoreCreateNotifications,
											CreateNotifications: &types.CreateNotificationsCommand{
												PromiseId: req.ResolvePromise.Id,
												Time:      t,
											},
										},
										{
											Kind: types.StoreDeleteSubscriptions,
											DeleteSubscriptions: &types.DeleteSubscriptionsCommand{
												PromiseId: req.ResolvePromise.Id,
											},
										},
									},
								},
							},
						}

						c.Yield(submission, func(t int64, completion *types.Completion, err error) {
							if err != nil {
								slog.Error("failed to update promise", "req", req, "err", err)
								res(t, nil, err)
								return
							}

							util.Assert(completion.Store != nil, "completion must not be nil")

							result := completion.Store.Results[0].UpdatePromise
							util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

							if result.RowsAffected == 1 {
								res(t, &types.Response{
									Kind: types.ResolvePromise,
									ResolvePromise: &types.ResolvePromiseResponse{
										Status: types.ResponseCreated,
										Promise: &promise.Promise{
											Id:          p.Id,
											State:       promise.Resolved,
											Param:       p.Param,
											Value:       req.ResolvePromise.Value,
											Timeout:     p.Timeout,
											CreatedOn:   p.CreatedOn,
											CompletedOn: &t,
										},
									},
								}, nil)
							} else {
								s.Add(ResolvePromise(t, req, res))
							}
						})
					}
				} else {
					var status types.ResponseStatus
					if p.State == promise.Resolved && p.Value.Ikey.Match(req.ResolvePromise.Value.Ikey) {
						status = types.ResponseOK
					} else {
						status = types.ResponseForbidden
					}

					res(t, &types.Response{
						Kind: types.ResolvePromise,
						ResolvePromise: &types.ResolvePromiseResponse{
							Status:  status,
							Promise: p,
						},
					}, nil)
				}
			}
		})
	})
}

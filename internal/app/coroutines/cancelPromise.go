package coroutines

import (
	"fmt"
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
)

func CancelPromise(t int64, req *types.Request, res func(int64, *types.Response, error)) *scheduler.Coroutine {
	return scheduler.NewCoroutine(fmt.Sprintf("CancelPromise(id=%s)", req.CancelPromise.Id), "CancelPromise", func(s *scheduler.Scheduler, c *scheduler.Coroutine) {
		if req.CancelPromise.Value.Headers == nil {
			req.CancelPromise.Value.Headers = map[string]string{}
		}
		if req.CancelPromise.Value.Data == nil {
			req.CancelPromise.Value.Data = []byte{}
		}

		submission := &types.Submission{
			Kind: types.Store,
			Store: &types.StoreSubmission{
				Transaction: &types.Transaction{
					Commands: []*types.Command{
						{
							Kind: types.StoreReadPromise,
							ReadPromise: &types.ReadPromiseCommand{
								Id: req.CancelPromise.Id,
							},
						},
					},
				},
			},
		}

		c.Yield(submission, func(t int64, completion *types.Completion, err error) {
			if err != nil {
				slog.Error("failed to read promise", "req", req, "err", err)
				res(t, nil, err)
				return
			}

			util.Assert(completion.Store != nil, "completion must not be nil")

			result := completion.Store.Results[0].ReadPromise
			util.Assert(result.RowsReturned == 0 || result.RowsReturned == 1, "result must return 0 or 1 rows")

			if result.RowsReturned == 0 {
				res(t, &types.Response{
					Kind: types.CancelPromise,
					CancelPromise: &types.CancelPromiseResponse{
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
						s.Add(TimeoutPromise(t, p, CancelPromise(t, req, res), func(t int64, err error) {
							if err != nil {
								slog.Error("failed to timeout promise", "req", req, "err", err)
								res(t, nil, err)
								return
							}

							res(t, &types.Response{
								Kind: types.CancelPromise,
								CancelPromise: &types.CancelPromiseResponse{
									Status: types.ResponseForbidden,
									Promise: &promise.Promise{
										Id:    p.Id,
										State: promise.Timedout,
										Param: p.Param,
										Value: promise.Value{
											Headers: map[string]string{},
											Data:    []byte{},
										},
										Timeout:                   p.Timeout,
										IdempotencyKeyForCreate:   p.IdempotencyKeyForCreate,
										IdempotencyKeyForComplete: p.IdempotencyKeyForComplete,
										Tags:                      p.Tags,
										CreatedOn:                 p.CreatedOn,
										CompletedOn:               &p.Timeout,
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
												Id:             req.CancelPromise.Id,
												State:          promise.Canceled,
												Value:          req.CancelPromise.Value,
												IdempotencyKey: req.CancelPromise.IdempotencyKey,
												CompletedOn:    t,
											},
										},
										{
											Kind: types.StoreCreateNotifications,
											CreateNotifications: &types.CreateNotificationsCommand{
												PromiseId: req.CancelPromise.Id,
												Time:      t,
											},
										},
										{
											Kind: types.StoreDeleteSubscriptions,
											DeleteSubscriptions: &types.DeleteSubscriptionsCommand{
												PromiseId: req.CancelPromise.Id,
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
									Kind: types.CancelPromise,
									CancelPromise: &types.CancelPromiseResponse{
										Status: types.ResponseCreated,
										Promise: &promise.Promise{
											Id:                        p.Id,
											State:                     promise.Canceled,
											Param:                     p.Param,
											Value:                     req.CancelPromise.Value,
											Timeout:                   p.Timeout,
											IdempotencyKeyForCreate:   p.IdempotencyKeyForCreate,
											IdempotencyKeyForComplete: req.CancelPromise.IdempotencyKey,
											Tags:                      p.Tags,
											CreatedOn:                 p.CreatedOn,
											CompletedOn:               &t,
										},
									},
								}, nil)
							} else {
								s.Add(CancelPromise(t, req, res))
							}
						})
					}
				} else {
					status := types.ResponseForbidden
					strict := req.CancelPromise.Strict && p.State != promise.Canceled

					if !strict && p.IdempotencyKeyForComplete.Match(req.CancelPromise.IdempotencyKey) {
						status = types.ResponseOK
					}

					res(t, &types.Response{
						Kind: types.CancelPromise,
						CancelPromise: &types.CancelPromiseResponse{
							Status:  status,
							Promise: p,
						},
					}, nil)
				}
			}
		})
	})
}

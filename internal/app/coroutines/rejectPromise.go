package coroutines

import (
	"fmt"
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
)

func RejectPromise(t int64, req *types.Request, res func(int64, *types.Response, error)) *scheduler.Coroutine {
	return scheduler.NewCoroutine(fmt.Sprintf("RejectPromise(id=%s)", req.RejectPromise.Id), "RejectPromise", func(s *scheduler.Scheduler, c *scheduler.Coroutine) {
		if req.RejectPromise.Value.Headers == nil {
			req.RejectPromise.Value.Headers = map[string]string{}
		}
		if req.RejectPromise.Value.Data == nil {
			req.RejectPromise.Value.Data = []byte{}
		}

		submission := &types.Submission{
			Kind: types.Store,
			Store: &types.StoreSubmission{
				Transaction: &types.Transaction{
					Commands: []*types.Command{
						{
							Kind: types.StoreReadPromise,
							ReadPromise: &types.ReadPromiseCommand{
								Id: req.RejectPromise.Id,
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
					Kind: types.RejectPromise,
					RejectPromise: &types.RejectPromiseResponse{
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
						s.Add(TimeoutPromise(t, p, RejectPromise(t, req, res), func(t int64, err error) {
							if err != nil {
								slog.Error("failed to timeout promise", "req", req, "err", err)
								res(t, nil, err)
								return
							}

							res(t, &types.Response{
								Kind: types.RejectPromise,
								RejectPromise: &types.RejectPromiseResponse{
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
											Kind: types.StoreReadSubscriptions,
											ReadSubscriptions: &types.ReadSubscriptionsCommand{
												PromiseIds: []string{req.RejectPromise.Id},
											},
										},
									},
								},
							},
						}

						c.Yield(submission, func(t int64, completion *types.Completion, err error) {
							if err != nil {
								slog.Error("failed to read subscriptions", "req", req, "err", err)
								res(t, nil, err)
								return
							}

							util.Assert(completion.Store != nil, "completion must not be nil")
							records := completion.Store.Results[0].ReadSubscriptions.Records

							commands := []*types.Command{
								{
									Kind: types.StoreUpdatePromise,
									UpdatePromise: &types.UpdatePromiseCommand{
										Id:          req.RejectPromise.Id,
										State:       promise.Rejected,
										Value:       req.RejectPromise.Value,
										CompletedOn: t,
									},
								},
								{
									Kind: types.StoreDeleteTimeout,
									DeleteTimeout: &types.DeleteTimeoutCommand{
										Id: req.RejectPromise.Id,
									},
								},
								{
									Kind: types.StoreDeleteSubscriptions,
									DeleteSubscriptions: &types.DeleteSubscriptionsCommand{
										PromiseId: req.RejectPromise.Id,
									},
								},
							}

							for _, record := range records {
								commands = append(commands, &types.Command{
									Kind: types.StoreCreateNotification,
									CreateNotification: &types.CreateNotificationCommand{
										Id:          record.Id,
										PromiseId:   record.PromiseId,
										Url:         record.Url,
										RetryPolicy: record.RetryPolicy,
										Time:        t,
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

							c.Yield(submission, func(t int64, completion *types.Completion, err error) {
								if err != nil {
									slog.Error("failed to update state", "req", req, "err", err)
									res(t, nil, err)
									return
								}

								util.Assert(completion.Store != nil, "completion must not be nil")

								result := completion.Store.Results[0].UpdatePromise
								util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

								if result.RowsAffected == 1 {
									res(t, &types.Response{
										Kind: types.RejectPromise,
										RejectPromise: &types.RejectPromiseResponse{
											Status: types.ResponseCreated,
											Promise: &promise.Promise{
												Id:          p.Id,
												State:       promise.Rejected,
												Param:       p.Param,
												Value:       req.RejectPromise.Value,
												Timeout:     p.Timeout,
												Tags:        p.Tags,
												CreatedOn:   p.CreatedOn,
												CompletedOn: &t,
											},
										},
									}, nil)
								} else {
									s.Add(RejectPromise(t, req, res))
								}
							})
						})
					}
				} else {
					var status types.ResponseStatus
					if p.State == promise.Rejected && p.Value.Ikey.Match(req.RejectPromise.Value.Ikey) {
						status = types.ResponseOK
					} else {
						status = types.ResponseForbidden
					}

					res(t, &types.Response{
						Kind: types.RejectPromise,
						RejectPromise: &types.RejectPromiseResponse{
							Status:  status,
							Promise: p,
						},
					}, nil)
				}
			}
		})
	})
}

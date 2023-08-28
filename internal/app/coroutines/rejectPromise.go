package coroutines

import (
	"fmt"
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
)

func RejectPromise(t int64, req *types.Request, res func(*types.Response, error)) *scheduler.Coroutine {
	return scheduler.NewCoroutine(fmt.Sprintf("RejectPromise(id=%s)", req.RejectPromise.Id), func(s *scheduler.Scheduler, c *scheduler.Coroutine) {
		if req.RejectPromise.Value.Headers == nil {
			req.RejectPromise.Value.Headers = map[string]string{}
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
				res(&types.Response{
					Kind: types.RejectPromise,
					RejectPromise: &types.RejectPromiseResponse{
						Status: types.ResponseNotFound,
					},
				}, nil)
			} else {
				record := result.Records[0]

				param, err := record.Param()
				if err != nil {
					slog.Error("failed to parse promise record param", "record", record, "err", err)
					res(nil, err)
					return
				}

				if record.State == promise.Pending {
					if t >= record.Timeout {
						s.Add(TimeoutPromise(t, req.RejectPromise.Id, RejectPromise(t, req, res), func(err error) {
							if err != nil {
								slog.Error("failed to timeout promise", "req", req, "err", err)
								res(nil, err)
								return
							}

							res(&types.Response{
								Kind: types.RejectPromise,
								RejectPromise: &types.RejectPromiseResponse{
									Status: types.ResponseForbidden,
									Promise: &promise.Promise{
										Id:    record.Id,
										State: promise.Timedout,
										Param: param,
										Value: promise.Value{
											Headers: map[string]string{},
											Ikey:    nil,
											Data:    nil,
										},
										Timeout: record.Timeout,
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

						c.Yield(submission, func(completion *types.Completion, err error) {
							if err != nil {
								slog.Error("failed to read subscriptions", "req", req, "err", err)
								res(nil, err)
								return
							}

							util.Assert(completion.Store != nil, "completion must not be nil")
							records := completion.Store.Results[0].ReadSubscriptions.Records

							commands := []*types.Command{
								{
									Kind: types.StoreUpdatePromise,
									UpdatePromise: &types.UpdatePromiseCommand{
										Id:    req.RejectPromise.Id,
										State: promise.Rejected,
										Value: req.RejectPromise.Value,
									},
								},
								{
									Kind: types.StoreDeleteTimeout,
									DeleteTimeout: &types.DeleteTimeoutCommand{
										Id: req.RejectPromise.Id,
									},
								},
							}

							for _, record := range records {
								commands = append(commands, &types.Command{
									Kind: types.StoreCreateNotification,
									CreateNotification: &types.CreateNotificationCommand{
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

							c.Yield(submission, func(completion *types.Completion, err error) {
								if err != nil {
									slog.Error("failed to update state", "req", req, "err", err)
									res(nil, err)
									return
								}

								util.Assert(completion.Store != nil, "completion must not be nil")

								result := completion.Store.Results[0].UpdatePromise
								util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

								if result.RowsAffected == 1 {
									res(&types.Response{
										Kind: types.RejectPromise,
										RejectPromise: &types.RejectPromiseResponse{
											Status: types.ResponseCreated,
											Promise: &promise.Promise{
												Id:      record.Id,
												State:   promise.Rejected,
												Timeout: record.Timeout,
												Param:   param,
												Value:   req.RejectPromise.Value,
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
					p, err := record.Promise()
					if err != nil {
						slog.Error("failed to parse promise record", "record", record, "err", err)
						res(nil, err)
						return
					}

					var status types.ResponseStatus
					if record.State == promise.Rejected && record.ValueIkey.Match(req.RejectPromise.Value.Ikey) {
						status = types.ResponseOK
					} else {
						status = types.ResponseForbidden
					}

					res(&types.Response{
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

package coroutines

import (
	"fmt"
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
)

func CompletePromise(t int64, req *types.Request, res func(int64, *types.Response, error)) *scheduler.Coroutine {
	return scheduler.NewCoroutine(fmt.Sprintf("CompletePromise(id=%s)", req.CompletePromise.Id), "CompletePromise", func(s *scheduler.Scheduler, c *scheduler.Coroutine) {
		util.Assert(req.CompletePromise.State.In(promise.Resolved|promise.Rejected|promise.Canceled), "state must be one of resolved, rejected, or canceled")

		if req.CompletePromise.Value.Headers == nil {
			req.CompletePromise.Value.Headers = map[string]string{}
		}
		if req.CompletePromise.Value.Data == nil {
			req.CompletePromise.Value.Data = []byte{}
		}

		submission := &types.Submission{
			Kind: types.Store,
			Store: &types.StoreSubmission{
				Transaction: &types.Transaction{
					Commands: []*types.Command{
						{
							Kind: types.StoreReadPromise,
							ReadPromise: &types.ReadPromiseCommand{
								Id: req.CompletePromise.Id,
							},
						},
						{
							Kind: types.StoreReadSubscriptions,
							ReadSubscriptions: &types.ReadSubscriptionsCommand{
								PromiseIds: []string{req.CompletePromise.Id},
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
			util.Assert(len(completion.Store.Results) == 2, "completion must contain two results")

			result := completion.Store.Results[0].ReadPromise
			records := completion.Store.Results[1].ReadSubscriptions.Records

			util.Assert(result.RowsReturned == 0 || result.RowsReturned == 1, "result must return 0 or 1 rows")

			if result.RowsReturned == 0 {
				res(t, &types.Response{
					Kind: types.CompletePromise,
					CompletePromise: &types.CompletePromiseResponse{
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
						s.Add(TimeoutPromise(t, p, records, CompletePromise(t, req, res), func(t int64, err error) {
							if err != nil {
								slog.Error("failed to timeout promise", "req", req, "err", err)
								res(t, nil, err)
								return
							}

							res(t, &types.Response{
								Kind: types.CompletePromise,
								CompletePromise: &types.CompletePromiseResponse{
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
						commands := []*types.Command{
							{
								Kind: types.StoreUpdatePromise,
								UpdatePromise: &types.UpdatePromiseCommand{
									Id:          req.CompletePromise.Id,
									State:       req.CompletePromise.State,
									Value:       req.CompletePromise.Value,
									CompletedOn: t,
								},
							},
							{
								Kind: types.StoreDeleteTimeout,
								DeleteTimeout: &types.DeleteTimeoutCommand{
									Id: req.CompletePromise.Id,
								},
							},
							{
								Kind: types.StoreDeleteSubscriptions,
								DeleteSubscriptions: &types.DeleteSubscriptionsCommand{
									PromiseId: req.CompletePromise.Id,
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
									Kind: types.CompletePromise,
									CompletePromise: &types.CompletePromiseResponse{
										Status: types.ResponseCreated,
										Promise: &promise.Promise{
											Id:          p.Id,
											State:       req.CompletePromise.State,
											Param:       p.Param,
											Value:       req.CompletePromise.Value,
											Timeout:     p.Timeout,
											Tags:        p.Tags,
											CreatedOn:   p.CreatedOn,
											CompletedOn: &t,
										},
									},
								}, nil)
							} else {
								s.Add(CompletePromise(t, req, res))
							}
						})
					}
				} else {
					var status types.ResponseStatus
					if p.Value.Ikey.Match(req.CompletePromise.Value.Ikey) {
						status = types.ResponseOK
					} else {
						status = types.ResponseForbidden
					}

					res(t, &types.Response{
						Kind: types.CompletePromise,
						CompletePromise: &types.CompletePromiseResponse{
							Status:  status,
							Promise: p,
						},
					}, nil)
				}
			}
		})
	})
}

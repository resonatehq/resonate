package coroutines

import (
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
)

func CancelPromise(req *t_api.Request, res func(*t_api.Response, error)) *scheduler.Coroutine {
	return scheduler.NewCoroutine("CancelPromise", func(s *scheduler.Scheduler, c *scheduler.Coroutine) {
		if req.CancelPromise.Value.Headers == nil {
			req.CancelPromise.Value.Headers = map[string]string{}
		}
		if req.CancelPromise.Value.Data == nil {
			req.CancelPromise.Value.Data = []byte{}
		}

		submission := &t_aio.Submission{
			Kind: t_aio.Store,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []*t_aio.Command{
						{
							Kind: t_aio.ReadPromise,
							ReadPromise: &t_aio.ReadPromiseCommand{
								Id: req.CancelPromise.Id,
							},
						},
					},
				},
			},
		}

		c.Yield(submission, func(completion *t_aio.Completion, err error) {
			if err != nil {
				slog.Error("failed to read promise", "req", req, "err", err)
				res(nil, err)
				return
			}

			util.Assert(completion.Store != nil, "completion must not be nil")

			result := completion.Store.Results[0].ReadPromise
			util.Assert(result.RowsReturned == 0 || result.RowsReturned == 1, "result must return 0 or 1 rows")

			if result.RowsReturned == 0 {
				res(&t_api.Response{
					Kind: t_api.CancelPromise,
					CancelPromise: &t_api.CancelPromiseResponse{
						Status: t_api.ResponseNotFound,
					},
				}, nil)
			} else {
				p, err := result.Records[0].Promise()
				if err != nil {
					slog.Error("failed to parse promise record", "record", result.Records[0], "err", err)
					res(nil, err)
					return
				}

				if p.State == promise.Pending {
					if s.Time() >= p.Timeout {
						s.Add(TimeoutPromise(p, CancelPromise(req, res), func(err error) {
							if err != nil {
								slog.Error("failed to timeout promise", "req", req, "err", err)
								res(nil, err)
								return
							}

							res(&t_api.Response{
								Kind: t_api.CancelPromise,
								CancelPromise: &t_api.CancelPromiseResponse{
									Status: t_api.ResponseForbidden,
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
						completedOn := s.Time()
						submission := &t_aio.Submission{
							Kind: t_aio.Store,
							Store: &t_aio.StoreSubmission{
								Transaction: &t_aio.Transaction{
									Commands: []*t_aio.Command{
										{
											Kind: t_aio.UpdatePromise,
											UpdatePromise: &t_aio.UpdatePromiseCommand{
												Id:             req.CancelPromise.Id,
												State:          promise.Canceled,
												Value:          req.CancelPromise.Value,
												IdempotencyKey: req.CancelPromise.IdempotencyKey,
												CompletedOn:    completedOn,
											},
										},
										{
											Kind: t_aio.CreateNotifications,
											CreateNotifications: &t_aio.CreateNotificationsCommand{
												PromiseId: req.CancelPromise.Id,
												Time:      completedOn,
											},
										},
										{
											Kind: t_aio.DeleteSubscriptions,
											DeleteSubscriptions: &t_aio.DeleteSubscriptionsCommand{
												PromiseId: req.CancelPromise.Id,
											},
										},
									},
								},
							},
						}

						c.Yield(submission, func(completion *t_aio.Completion, err error) {
							if err != nil {
								slog.Error("failed to update promise", "req", req, "err", err)
								res(nil, err)
								return
							}

							util.Assert(completion.Store != nil, "completion must not be nil")

							result := completion.Store.Results[0].UpdatePromise
							util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

							if result.RowsAffected == 1 {
								res(&t_api.Response{
									Kind: t_api.CancelPromise,
									CancelPromise: &t_api.CancelPromiseResponse{
										Status: t_api.ResponseCreated,
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
											CompletedOn:               &completedOn,
										},
									},
								}, nil)
							} else {
								s.Add(CancelPromise(req, res))
							}
						})
					}
				} else {
					status := t_api.ResponseForbidden
					strict := req.CancelPromise.Strict && p.State != promise.Canceled

					if !strict && p.IdempotencyKeyForComplete.Match(req.CancelPromise.IdempotencyKey) {
						status = t_api.ResponseOK
					}

					res(&t_api.Response{
						Kind: t_api.CancelPromise,
						CancelPromise: &t_api.CancelPromiseResponse{
							Status:  status,
							Promise: p,
						},
					}, nil)
				}
			}
		})
	})
}

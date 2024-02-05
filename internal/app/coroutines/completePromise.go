package coroutines

import (
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/metadata"
	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
)

func CompletePromise(metadata *metadata.Metadata, req *t_api.Request, res func(*t_api.Response, error)) *scheduler.Coroutine[*t_aio.Completion, *t_aio.Submission] {
	return scheduler.NewCoroutine(metadata, func(c *scheduler.Coroutine[*t_aio.Completion, *t_aio.Submission]) {
		if req.CompletePromise.Value.Headers == nil {
			req.CompletePromise.Value.Headers = map[string]string{}
		}
		if req.CompletePromise.Value.Data == nil {
			req.CompletePromise.Value.Data = []byte{}
		}

		completion, err := c.Yield(&t_aio.Submission{
			Kind: t_aio.Store,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []*t_aio.Command{
						{
							Kind: t_aio.ReadPromise,
							ReadPromise: &t_aio.ReadPromiseCommand{
								Id: req.CompletePromise.Id,
							},
						},
					},
				},
			},
		})

		if err != nil {
			slog.Error("failed to read promise", "req", req, "err", err)
			res(nil, t_api.NewResonateError(t_api.ErrAIOStoreFailure, "failed to read promise", err))
			return
		}

		util.Assert(completion.Store != nil, "completion must not be nil")

		result := completion.Store.Results[0].ReadPromise
		util.Assert(result.RowsReturned == 0 || result.RowsReturned == 1, "result must return 0 or 1 rows")

		if result.RowsReturned == 0 {
			res(&t_api.Response{
				Kind: req.Kind,
				CompletePromise: &t_api.CompletePromiseResponse{
					Status: t_api.StatusPromiseNotFound,
				},
			}, nil)
		} else {
			p, err := result.Records[0].Promise()
			if err != nil {
				slog.Error("failed to parse promise record", "record", result.Records[0], "err", err)
				res(nil, t_api.NewResonateError(t_api.ErrAIOStoreSerializationFailure, "failed to parse promise record", err))
				return
			}

			if p.State == promise.Pending {
				if c.Time() >= p.Timeout {
					c.Scheduler.Add(TimeoutPromise(metadata, p, CompletePromise(metadata, req, res), func(err error) {
						if err != nil {
							slog.Error("failed to timeout promise", "req", req, "err", err)
							res(nil, t_api.NewResonateError(t_api.ErrAIOStoreFailure, "failed to timeout promise", err))
							return
						}

						res(&t_api.Response{
							Kind: req.Kind,
							CompletePromise: &t_api.CompletePromiseResponse{
								Status: t_api.StatusPromiseAlreadyTimedOut,
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
					completedOn := c.Time()
					completion, err := c.Yield(&t_aio.Submission{
						Kind: t_aio.Store,
						Store: &t_aio.StoreSubmission{
							Transaction: &t_aio.Transaction{
								Commands: []*t_aio.Command{
									{
										Kind: t_aio.UpdatePromise,
										UpdatePromise: &t_aio.UpdatePromiseCommand{
											Id:             req.CompletePromise.Id,
											State:          req.CompletePromise.State,
											Value:          req.CompletePromise.Value,
											IdempotencyKey: req.CompletePromise.IdempotencyKey,
											CompletedOn:    completedOn,
										},
									},
									{
										Kind: t_aio.CreateNotifications,
										CreateNotifications: &t_aio.CreateNotificationsCommand{
											PromiseId: req.CompletePromise.Id,
											Time:      completedOn,
										},
									},
									{
										Kind: t_aio.DeleteSubscriptions,
										DeleteSubscriptions: &t_aio.DeleteSubscriptionsCommand{
											PromiseId: req.CompletePromise.Id,
										},
									},
								},
							},
						},
					})

					if err != nil {
						slog.Error("failed to update promise", "req", req, "err", err)
						res(nil, t_api.NewResonateError(t_api.ErrAIOStoreFailure, "failed to update promise", err))
						return
					}

					util.Assert(completion.Store != nil, "completion must not be nil")

					result := completion.Store.Results[0].UpdatePromise
					util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

					if result.RowsAffected == 1 {
						res(&t_api.Response{
							Kind: req.Kind,
							CompletePromise: &t_api.CompletePromiseResponse{
								Status: t_api.StatusCreated,
								Promise: &promise.Promise{
									Id:                        p.Id,
									State:                     req.CompletePromise.State,
									Param:                     p.Param,
									Value:                     req.CompletePromise.Value,
									Timeout:                   p.Timeout,
									IdempotencyKeyForCreate:   p.IdempotencyKeyForCreate,
									IdempotencyKeyForComplete: req.CompletePromise.IdempotencyKey,
									Tags:                      p.Tags,
									CreatedOn:                 p.CreatedOn,
									CompletedOn:               &completedOn,
								},
							},
						}, nil)
					} else {
						c.Scheduler.Add(CompletePromise(metadata, req, res))
					}
				}
			} else {
				status := t_api.ForbiddenStatus(p.State)
				strict := req.CompletePromise.Strict && p.State != req.CompletePromise.State
				timeout := !req.CompletePromise.Strict && p.State == promise.Timedout

				if (!strict && p.IdempotencyKeyForComplete.Match(req.CompletePromise.IdempotencyKey)) || timeout {
					status = t_api.StatusOK
				}

				res(&t_api.Response{
					Kind: req.Kind,
					CompletePromise: &t_api.CompletePromiseResponse{
						Status:  status,
						Promise: p,
					},
				}, nil)
			}
		}
	})
}

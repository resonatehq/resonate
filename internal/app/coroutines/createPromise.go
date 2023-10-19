package coroutines

import (
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
)

func CreatePromise(req *t_api.Request, res func(*t_api.Response, error)) *scheduler.Coroutine[*t_aio.Completion, *t_aio.Submission] {
	return scheduler.NewCoroutine("CreatePromise", func(c *scheduler.Coroutine[*t_aio.Completion, *t_aio.Submission]) {
		if req.CreatePromise.Param.Headers == nil {
			req.CreatePromise.Param.Headers = map[string]string{}
		}
		if req.CreatePromise.Param.Data == nil {
			req.CreatePromise.Param.Data = []byte{}
		}
		if req.CreatePromise.Tags == nil {
			req.CreatePromise.Tags = map[string]string{}
		}

		completion, err := c.Yield(&t_aio.Submission{
			Kind: t_aio.Store,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []*t_aio.Command{
						{
							Kind: t_aio.ReadPromise,
							ReadPromise: &t_aio.ReadPromiseCommand{
								Id: req.CreatePromise.Id,
							},
						},
					},
				},
			},
		})

		if err != nil {
			slog.Error("failed to read promise", "req", req, "err", err)
			res(nil, err)
			return
		}

		util.Assert(completion.Store != nil, "completion must not be nil")

		result := completion.Store.Results[0].ReadPromise
		util.Assert(result.RowsReturned == 0 || result.RowsReturned == 1, "result must return 0 or 1 rows")

		if result.RowsReturned == 0 {
			createdOn := c.Time()
			completion, err := c.Yield(&t_aio.Submission{
				Kind: t_aio.Store,
				Store: &t_aio.StoreSubmission{
					Transaction: &t_aio.Transaction{
						Commands: []*t_aio.Command{
							{
								Kind: t_aio.CreatePromise,
								CreatePromise: &t_aio.CreatePromiseCommand{
									Id:             req.CreatePromise.Id,
									Param:          req.CreatePromise.Param,
									Timeout:        req.CreatePromise.Timeout,
									IdempotencyKey: req.CreatePromise.IdempotencyKey,
									Tags:           req.CreatePromise.Tags,
									CreatedOn:      createdOn,
								},
							},
						},
					},
				},
			})

			if err != nil {
				slog.Error("failed to update promise", "req", req, "err", err)
				res(nil, err)
				return
			}

			util.Assert(completion.Store != nil, "completion must not be nil")

			result := completion.Store.Results[0].CreatePromise
			util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

			if result.RowsAffected == 1 {
				res(&t_api.Response{
					Kind: t_api.CreatePromise,
					CreatePromise: &t_api.CreatePromiseResponse{
						Status: t_api.ResponseCreated,
						Promise: &promise.Promise{
							Id:                      req.CreatePromise.Id,
							State:                   promise.Pending,
							Param:                   req.CreatePromise.Param,
							Timeout:                 req.CreatePromise.Timeout,
							IdempotencyKeyForCreate: req.CreatePromise.IdempotencyKey,
							Tags:                    req.CreatePromise.Tags,
							CreatedOn:               &createdOn,
						},
					},
				}, nil)
			} else {
				c.Scheduler.Add(CreatePromise(req, res))
			}
		} else {
			p, err := result.Records[0].Promise()
			if err != nil {
				slog.Error("failed to parse promise record", "record", result.Records[0], "err", err)
				res(nil, err)
				return
			}

			status := t_api.ResponseForbidden
			strict := req.CreatePromise.Strict && p.State != promise.Pending

			if !strict && p.IdempotencyKeyForCreate.Match(req.CreatePromise.IdempotencyKey) {
				status = t_api.ResponseOK
			}

			if p.State == promise.Pending && c.Time() >= p.Timeout {
				c.Scheduler.Add(TimeoutPromise(p, CreatePromise(req, res), func(err error) {
					if err != nil {
						slog.Error("failed to timeout promise", "req", req, "err", err)
						res(nil, err)
						return
					}

					res(&t_api.Response{
						Kind: t_api.CreatePromise,
						CreatePromise: &t_api.CreatePromiseResponse{
							Status: status,
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
				res(&t_api.Response{
					Kind: t_api.CreatePromise,
					CreatePromise: &t_api.CreatePromiseResponse{
						Status:  status,
						Promise: p,
					},
				}, nil)
			}
		}
	})
}

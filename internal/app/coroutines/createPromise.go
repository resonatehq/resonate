package coroutines

import (
	"errors"
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
)

func CreatePromise(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	if r.CreatePromise.Param.Headers == nil {
		r.CreatePromise.Param.Headers = map[string]string{}
	}
	if r.CreatePromise.Param.Data == nil {
		r.CreatePromise.Param.Data = []byte{}
	}
	if r.CreatePromise.Tags == nil {
		r.CreatePromise.Tags = map[string]string{}
	}

	completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
		Kind: t_aio.Store,
		Tags: r.Tags,
		Store: &t_aio.StoreSubmission{
			Transaction: &t_aio.Transaction{
				Commands: []*t_aio.Command{
					{
						Kind: t_aio.ReadPromise,
						ReadPromise: &t_aio.ReadPromiseCommand{
							Id: r.CreatePromise.Id,
						},
					},
				},
			},
		},
	})

	if err != nil {
		slog.Error("failed to read promise", "req", r, "err", err)
		return nil, t_api.NewResonateError(t_api.ErrAIOStoreFailure, "failed to read promise", err)
	}

	util.Assert(completion.Store != nil, "completion must not be nil")

	result := completion.Store.Results[0].ReadPromise
	util.Assert(result.RowsReturned == 0 || result.RowsReturned == 1, "result must return 0 or 1 rows")

	var res *t_api.Response

	if result.RowsReturned == 0 {
		createdOn := c.Time()

		createCmds := []*t_aio.Command{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:             r.CreatePromise.Id,
					State:          promise.Pending,
					Param:          r.CreatePromise.Param,
					Timeout:        r.CreatePromise.Timeout,
					IdempotencyKey: r.CreatePromise.IdempotencyKey,
					Tags:           r.CreatePromise.Tags,
					CreatedOn:      createdOn,
				},
			},
		}

		_, err := queuing.CoroutineRouter().Match(r.CreatePromise.Id)
		if err != nil {
			if !errors.Is(err, queuing.ErrRouteDoesNotMatchAnyPattern) {
				panic(err)
			}
		}

		if err == nil {
			createCmds = append(createCmds, &t_aio.Command{
				Kind: t_aio.CreateTask,
				CreateTask: &t_aio.CreateTaskCommand{
					Id:             r.CreatePromise.Id,
					Counter:        0,
					PromiseId:      r.CreatePromise.Id,
					ClaimTimeout:   createdOn, // Set it to the createdOn time to trigger immediately.
					PromiseTimeout: r.CreatePromise.Timeout,
					CreatedOn:      createdOn,
				},
			})
		}

		completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
			Kind: t_aio.Store,
			Tags: r.Tags,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: createCmds,
				},
			},
		})

		if err != nil {
			slog.Error("failed to update promise", "req", r, "err", err)
			return nil, t_api.NewResonateError(t_api.ErrAIOStoreFailure, "failed to update promise", err)
		}

		util.Assert(completion.Store != nil, "completion must not be nil")

		result := completion.Store.Results[0].CreatePromise
		util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

		if result.RowsAffected == 1 {
			res = &t_api.Response{
				Kind: t_api.CreatePromise,
				Tags: r.Tags,
				CreatePromise: &t_api.CreatePromiseResponse{
					Status: t_api.StatusCreated,
					Promise: &promise.Promise{
						Id:                      r.CreatePromise.Id,
						State:                   promise.Pending,
						Param:                   r.CreatePromise.Param,
						Timeout:                 r.CreatePromise.Timeout,
						IdempotencyKeyForCreate: r.CreatePromise.IdempotencyKey,
						Tags:                    r.CreatePromise.Tags,
						CreatedOn:               &createdOn,
					},
				},
			}
		} else {
			// It's possible that the promise was created by another coroutine
			// while we were timing out. In that case, we should just retry.
			return CreatePromise(c, r)
		}
	} else {
		p, err := result.Records[0].Promise()
		if err != nil {
			slog.Error("failed to parse promise record", "record", result.Records[0], "err", err)
			return nil, t_api.NewResonateError(t_api.ErrAIOStoreSerializationFailure, "failed to parse promise record", err)
		}

		// initial status
		status := t_api.StatusPromiseAlreadyExists

		if p.State == promise.Pending && c.Time() >= p.Timeout {
			success, err := gocoro.SpawnAndAwait(c, TimeoutPromise(p))
			if err != nil {
				return nil, err
			}

			if !success {
				// It's possible that the promise was created by another coroutine
				// while we were timing out. In that case, we should just retry.
				return CreatePromise(c, r)
			}

			// switch status to ok if not strict and idempotency keys match
			if !r.CreatePromise.Strict && p.IdempotencyKeyForCreate.Match(r.CreatePromise.IdempotencyKey) {
				status = t_api.StatusOK
			}

			res = &t_api.Response{
				Kind: r.Kind,
				Tags: r.Tags,
				CreatePromise: &t_api.CreatePromiseResponse{
					Status: status,
					Promise: &promise.Promise{
						Id:    p.Id,
						State: promise.GetTimedoutState(p),
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
			}
		} else {
			// switch status to ok if not strict and idempotency keys match
			strict := r.CreatePromise.Strict && p.State != promise.Pending
			if !strict && p.IdempotencyKeyForCreate.Match(r.CreatePromise.IdempotencyKey) {
				status = t_api.StatusOK
			}

			res = &t_api.Response{
				Kind: t_api.CreatePromise,
				Tags: r.Tags,
				CreatePromise: &t_api.CreatePromiseResponse{
					Status:  status,
					Promise: p,
				},
			}
		}
	}

	util.Assert(res != nil, "response must not be nil")
	return res, nil
}

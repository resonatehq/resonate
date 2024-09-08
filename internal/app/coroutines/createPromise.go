package coroutines

import (
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
)

func CreatePromise(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	// first read the promise to see if it already exists
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
		return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
	}

	util.Assert(completion.Store != nil, "completion must not be nil")

	result := completion.Store.Results[0].ReadPromise
	util.Assert(result.RowsReturned == 0 || result.RowsReturned == 1, "result must return 0 or 1 rows")

	var res *t_api.Response

	if result.RowsReturned == 0 {
		cmd := &t_aio.CreatePromiseCommand{
			Id:             r.CreatePromise.Id,
			Param:          r.CreatePromise.Param,
			Timeout:        r.CreatePromise.Timeout,
			IdempotencyKey: r.CreatePromise.IdempotencyKey,
			Tags:           r.CreatePromise.Tags,
			CreatedOn:      c.Time(),
		}

		// if the promise does not exist, create it
		ok, err := gocoro.SpawnAndAwait(c, createPromise(cmd))
		if err != nil {
			return nil, err
		}

		if !ok {
			// It's possible that the promise was created by another coroutine
			// while we were creating. In that case, we should just retry.
			return CreatePromise(c, r)
		}

		res = &t_api.Response{
			Kind: t_api.CreatePromise,
			Tags: r.Tags,
			CreatePromise: &t_api.CreatePromiseResponse{
				Status: t_api.StatusCreated,
				Promise: &promise.Promise{
					Id:                      cmd.Id,
					State:                   promise.Pending,
					Param:                   cmd.Param,
					Timeout:                 cmd.Timeout,
					IdempotencyKeyForCreate: cmd.IdempotencyKey,
					Tags:                    cmd.Tags,
					CreatedOn:               &cmd.CreatedOn,
				},
			},
		}
	} else {
		p, err := result.Records[0].Promise()
		if err != nil {
			slog.Error("failed to parse promise record", "record", result.Records[0], "err", err)
			return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
		}

		// initial status
		status := t_api.StatusPromiseAlreadyExists

		if p.State == promise.Pending && c.Time() >= p.Timeout {
			ok, err := gocoro.SpawnAndAwait(c, completePromise(p.Timeout, &t_api.Request{
				Kind: t_api.CompletePromise,
				Tags: r.Tags,
				CompletePromise: &t_api.CompletePromiseRequest{
					Id:    p.Id,
					State: promise.GetTimedoutState(p),
				},
			}))
			if err != nil {
				return nil, err
			}

			if !ok {
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
						Id:                        p.Id,
						State:                     promise.GetTimedoutState(p),
						Param:                     p.Param,
						Value:                     promise.Value{Headers: map[string]string{}, Data: []byte{}},
						Timeout:                   p.Timeout,
						IdempotencyKeyForCreate:   p.IdempotencyKeyForCreate,
						IdempotencyKeyForComplete: nil,
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

func createPromise(createPromise *t_aio.CreatePromiseCommand, additionalCmds ...*t_aio.Command) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, bool] {
	if createPromise.Param.Headers == nil {
		createPromise.Param.Headers = map[string]string{}
	}
	if createPromise.Param.Data == nil {
		createPromise.Param.Data = []byte{}
	}
	if createPromise.Tags == nil {
		createPromise.Tags = map[string]string{}
	}

	return func(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, bool]) (bool, error) {
		// add create promise command
		commands := []*t_aio.Command{{
			Kind:          t_aio.CreatePromise,
			CreatePromise: createPromise,
		}}

		// add additional commands
		commands = append(commands, additionalCmds...)

		// check matcher to see if a task needs to be created
		completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
			Kind: t_aio.Match,
			Tags: createPromise.Tags,
			Match: &t_aio.MatchSubmission{
				Promise: &promise.Promise{
					Id:                      createPromise.Id,
					State:                   promise.Pending,
					Param:                   createPromise.Param,
					Timeout:                 createPromise.Timeout,
					IdempotencyKeyForCreate: createPromise.IdempotencyKey,
					Tags:                    createPromise.Tags,
					CreatedOn:               &createPromise.CreatedOn,
				},
			},
		})

		if err != nil {
			slog.Warn("failed to match promise", "req", createPromise, "err", err)
		}

		if err == nil && completion.Match.Matched {
			util.Assert(completion.Match.Command != nil, "command must not be nil")
			completion.Match.Command.CreatedOn = createPromise.CreatedOn

			// add create task command if matched
			additionalCmds = append(additionalCmds, &t_aio.Command{
				Kind:       t_aio.CreateTask,
				CreateTask: completion.Match.Command,
			})
		}

		// yield commands
		completion, err = gocoro.YieldAndAwait(c, &t_aio.Submission{
			Kind: t_aio.Store,
			Tags: createPromise.Tags,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: commands,
				},
			},
		})

		if err != nil {
			slog.Error("failed to update promise", "req", createPromise, "err", err)
			return false, t_api.NewError(t_api.StatusAIOStoreError, err)
		}

		util.Assert(completion.Store != nil, "completion must not be nil")
		util.Assert(len(completion.Store.Results) == len(commands), "completion must have one or two results")
		util.Assert(completion.Store.Results[0].CreatePromise.RowsAffected == 0 || completion.Store.Results[0].CreatePromise.RowsAffected == 1, "result must return 0 or 1 rows")

		return completion.Store.Results[0].CreatePromise.RowsAffected == 1, nil
	}
}

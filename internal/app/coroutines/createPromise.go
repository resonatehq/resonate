package coroutines

import (
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/callback"
	"github.com/resonatehq/resonate/pkg/message"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/task"
)

func CreatePromise(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	util.Assert(r.CreatePromise.Task == nil || r.CreatePromise.Callback == nil, "one of task or callback must be nil")

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

		var additionalCmds []*t_aio.Command

		if r.CreatePromise.Task != nil {
			// create task
			additionalCmds = []*t_aio.Command{{
				Kind: t_aio.CreateTask,
				CreateTask: &t_aio.CreateTaskCommand{
					Recv:       r.CreatePromise.Task.Recv,
					Mesg:       &message.Mesg{Type: message.Invoke, Root: r.CreatePromise.Id, Leaf: r.CreatePromise.Id},
					Timeout:    r.CreatePromise.Timeout,
					ProcessId:  &r.CreatePromise.Task.ProcessId,
					State:      task.Claimed,
					Frequency:  r.CreatePromise.Task.Frequency,
					Expiration: c.Time() + int64(r.CreatePromise.Task.Frequency),
					CreatedOn:  c.Time(),
				},
			}}
		} else if r.CreatePromise.Callback != nil {
			// create callback
			additionalCmds = []*t_aio.Command{{
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.CreateCallbackCommand{
					PromiseId: cmd.Id,
					Recv:      r.CreatePromise.Callback.Recv,
					Mesg:      &message.Mesg{Type: message.Resume, Root: r.CreatePromise.Callback.RootPromiseId, Leaf: r.CreatePromise.Id},
					Timeout:   r.CreatePromise.Callback.Timeout,
					CreatedOn: c.Time(),
				},
			}}
		}

		// if the promise does not exist, create it
		completion, err := gocoro.SpawnAndAwait(c, createPromise(r.Tags, cmd, additionalCmds...))
		if err != nil {
			return nil, err
		}

		if completion.Store.Results[0].CreatePromise.RowsAffected == 0 {
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

		if r.CreatePromise.Task != nil {
			util.Assert(additionalCmds[0].Kind == t_aio.CreateTask, "command must be create task")
			util.Assert(completion.Store.Results[1].Kind == t_aio.CreateTask, "completion must be create task")

			cmd := additionalCmds[0].CreateTask

			res.CreatePromise.Task = &task.Task{
				Id:         completion.Store.Results[1].CreateTask.LastInsertId,
				ProcessId:  cmd.ProcessId,
				State:      cmd.State,
				Recv:       cmd.Recv,
				Mesg:       cmd.Mesg,
				Timeout:    cmd.Timeout,
				Counter:    0,
				Attempt:    0,
				Frequency:  cmd.Frequency,
				Expiration: cmd.Expiration,
				CreatedOn:  &cmd.CreatedOn,
			}
		} else if r.CreatePromise.Callback != nil {
			util.Assert(additionalCmds[0].Kind == t_aio.CreateCallback, "command must be create callback")
			util.Assert(completion.Store.Results[1].Kind == t_aio.CreateCallback, "completion must be create callback")

			cmd := additionalCmds[0].CreateCallback

			res.CreatePromise.Callback = &callback.Callback{
				Id:        completion.Store.Results[1].CreateCallback.LastInsertId,
				PromiseId: cmd.PromiseId,
				Recv:      cmd.Recv,
				Mesg:      cmd.Mesg,
				Timeout:   cmd.Timeout,
				CreatedOn: cmd.CreatedOn,
			}
		}
	} else {
		p, err := result.Records[0].Promise()
		if err != nil {
			slog.Error("failed to parse promise record", "record", result.Records[0], "err", err)
			return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
		}

		// initial status
		status := t_api.StatusPromiseAlreadyExists

		if p.State == promise.Pending && p.Timeout <= c.Time() {
			cmd := &t_aio.UpdatePromiseCommand{
				Id:             r.CreatePromise.Id,
				State:          promise.GetTimedoutState(p),
				Value:          promise.Value{},
				IdempotencyKey: nil,
				CompletedOn:    p.Timeout,
			}

			ok, err := gocoro.SpawnAndAwait(c, completePromise(r.Tags, cmd))
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
						State:                     cmd.State,
						Param:                     p.Param,
						Value:                     cmd.Value,
						Timeout:                   p.Timeout,
						IdempotencyKeyForCreate:   p.IdempotencyKeyForCreate,
						IdempotencyKeyForComplete: cmd.IdempotencyKey,
						Tags:                      p.Tags,
						CreatedOn:                 p.CreatedOn,
						CompletedOn:               &cmd.CompletedOn,
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

func createPromise(tags map[string]string, cmd *t_aio.CreatePromiseCommand, additionalCmds ...*t_aio.Command) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, *t_aio.Completion] {
	if cmd.Param.Headers == nil {
		cmd.Param.Headers = map[string]string{}
	}
	if cmd.Param.Data == nil {
		cmd.Param.Data = []byte{}
	}
	if cmd.Tags == nil {
		cmd.Tags = map[string]string{}
	}

	return func(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, *t_aio.Completion]) (*t_aio.Completion, error) {
		// add create promise command
		commands := []*t_aio.Command{{
			Kind:          t_aio.CreatePromise,
			CreatePromise: cmd,
		}}

		// add additional commands
		commands = append(commands, additionalCmds...)

		// check router to see if a task needs to be created
		completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
			Kind: t_aio.Router,
			Tags: tags,
			Router: &t_aio.RouterSubmission{
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
		})

		if err != nil {
			slog.Warn("failed to match promise", "cmd", cmd, "err", err)
		}

		if err == nil && completion.Router.Matched {
			util.Assert(completion.Router.Recv != nil, "recv must not be nil")

			// add create task command if matched
			commands = append(commands, &t_aio.Command{
				Kind: t_aio.CreateTask,
				CreateTask: &t_aio.CreateTaskCommand{
					Recv:      completion.Router.Recv,
					Mesg:      &message.Mesg{Type: message.Invoke, Root: cmd.Id, Leaf: cmd.Id},
					Timeout:   cmd.Timeout,
					State:     task.Init,
					CreatedOn: cmd.CreatedOn,
				},
			})
		}

		// yield commands
		completion, err = gocoro.YieldAndAwait(c, &t_aio.Submission{
			Kind: t_aio.Store,
			Tags: tags,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: commands,
				},
			},
		})

		if err != nil {
			slog.Error("failed to create promise", "err", err)
			return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
		}

		util.Assert(completion.Store != nil, "completion must not be nil")
		util.Assert(len(completion.Store.Results) == len(commands), "completion must have same number of results as commands")
		util.Assert(completion.Store.Results[0].CreatePromise.RowsAffected == 0 || completion.Store.Results[0].CreatePromise.RowsAffected == 1, "result must return 0 or 1 rows")

		return completion, nil
	}
}

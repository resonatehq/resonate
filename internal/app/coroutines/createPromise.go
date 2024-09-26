package coroutines

import (
	"fmt"
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
	// request data
	var createPromiseReq *t_api.CreatePromiseRequest
	var createTaskReq *t_api.CreateTaskRequest
	var createCallbackReq *t_api.CreateCallbackRequest

	// response status
	var status t_api.StatusCode

	// response data
	var p *promise.Promise
	var t *task.Task
	var cb *callback.Callback

	switch r.Kind {
	case t_api.CreatePromise:
		createPromiseReq = r.CreatePromise
	case t_api.CreatePromiseAndTask:
		createPromiseReq = r.CreatePromiseAndTask.Promise
		createTaskReq = r.CreatePromiseAndTask.Task
	case t_api.CreatePromiseAndCallback:
		createPromiseReq = r.CreatePromiseAndCallback.Promise
		createCallbackReq = r.CreatePromiseAndCallback.Callback
	default:
		panic(fmt.Sprintf("invalid kind '%s'", r.Kind))
	}

	util.Assert(createPromiseReq != nil, "create promise must not be nil")
	util.Assert(r.Kind != t_api.CreatePromiseAndTask || createTaskReq != nil, "create task must not be nil")
	util.Assert(r.Kind != t_api.CreatePromiseAndCallback || createCallbackReq != nil, "create callback must not be nil")

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
							Id: createPromiseReq.Id,
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

	if result.RowsReturned == 0 {
		cmd := &t_aio.CreatePromiseCommand{
			Id:             createPromiseReq.Id,
			Param:          createPromiseReq.Param,
			Timeout:        createPromiseReq.Timeout,
			IdempotencyKey: createPromiseReq.IdempotencyKey,
			Tags:           createPromiseReq.Tags,
			CreatedOn:      c.Time(),
		}

		var additionalCmds []*t_aio.Command

		switch r.Kind {
		case t_api.CreatePromiseAndTask:
			// create task
			additionalCmds = []*t_aio.Command{{
				Kind: t_aio.CreateTask,
				CreateTask: &t_aio.CreateTaskCommand{
					Recv:       createTaskReq.Recv,
					Mesg:       &message.Mesg{Type: message.Invoke, Root: createTaskReq.PromiseId, Leaf: createTaskReq.PromiseId},
					Timeout:    createPromiseReq.Timeout,
					ProcessId:  &createTaskReq.ProcessId,
					State:      task.Claimed,
					Frequency:  createTaskReq.Frequency,
					Expiration: c.Time() + int64(createTaskReq.Frequency),
					CreatedOn:  c.Time(),
				},
			}}
		case t_api.CreatePromiseAndCallback:
			// create callback
			additionalCmds = []*t_aio.Command{{
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.CreateCallbackCommand{
					PromiseId: createCallbackReq.PromiseId,
					Recv:      createCallbackReq.Recv,
					Mesg:      &message.Mesg{Type: message.Resume, Root: createCallbackReq.RootPromiseId, Leaf: createCallbackReq.PromiseId},
					Timeout:   createCallbackReq.Timeout,
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

		// set status
		status = t_api.StatusCreated

		// set promise
		p = &promise.Promise{
			Id:                      cmd.Id,
			State:                   promise.Pending,
			Param:                   cmd.Param,
			Timeout:                 cmd.Timeout,
			IdempotencyKeyForCreate: cmd.IdempotencyKey,
			Tags:                    cmd.Tags,
			CreatedOn:               &cmd.CreatedOn,
		}

		switch r.Kind {
		case t_api.CreatePromiseAndTask:
			util.Assert(additionalCmds[0].Kind == t_aio.CreateTask, "command must be create task")
			util.Assert(completion.Store.Results[1].Kind == t_aio.CreateTask, "completion must be create task")
			cmd := additionalCmds[0].CreateTask

			t = &task.Task{
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
		case t_api.CreatePromiseAndCallback:
			util.Assert(additionalCmds[0].Kind == t_aio.CreateCallback, "command must be create callback")
			util.Assert(completion.Store.Results[1].Kind == t_aio.CreateCallback, "completion must be create callback")
			cmd := additionalCmds[0].CreateCallback

			cb = &callback.Callback{
				Id:        completion.Store.Results[1].CreateCallback.LastInsertId,
				PromiseId: cmd.PromiseId,
				Recv:      cmd.Recv,
				Mesg:      cmd.Mesg,
				Timeout:   cmd.Timeout,
				CreatedOn: cmd.CreatedOn,
			}
		}
	} else {
		p, err = result.Records[0].Promise()
		if err != nil {
			slog.Error("failed to parse promise record", "record", result.Records[0], "err", err)
			return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
		}

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

			// set status to ok if not strict and idempotency keys match
			if !createPromiseReq.Strict && p.IdempotencyKeyForCreate.Match(createPromiseReq.IdempotencyKey) {
				status = t_api.StatusOK
			} else {
				status = t_api.StatusPromiseAlreadyExists
			}

			// update promise
			p.State = cmd.State
			p.Value = cmd.Value
			p.IdempotencyKeyForComplete = cmd.IdempotencyKey
			p.CompletedOn = &cmd.CompletedOn
		} else if !(createPromiseReq.Strict && p.State != promise.Pending) && p.IdempotencyKeyForCreate.Match(r.CreatePromise.IdempotencyKey) {
			// switch status to ok if not strict and idempotency keys match
			status = t_api.StatusOK
		} else {
			status = t_api.StatusPromiseAlreadyExists
		}
	}

	switch r.Kind {
	case t_api.CreatePromise:
		return &t_api.Response{
			Kind:          t_api.CreatePromise,
			Tags:          r.Tags,
			CreatePromise: &t_api.CreatePromiseResponse{Status: status, Promise: p},
		}, nil
	case t_api.CreatePromiseAndTask:
		return &t_api.Response{
			Kind:                 t_api.CreatePromiseAndTask,
			Tags:                 r.Tags,
			CreatePromiseAndTask: &t_api.CreatePromiseAndTaskResponse{Status: status, Promise: p, Task: t},
		}, nil
	case t_api.CreatePromiseAndCallback:
		return &t_api.Response{
			Kind:                     t_api.CreatePromiseAndCallback,
			Tags:                     r.Tags,
			CreatePromiseAndCallback: &t_api.CreatePromiseAndCallbackResponse{Status: status, Promise: p, Callback: cb},
		}, nil
	default:
		panic(fmt.Sprintf("invalid kind '%s'", r.Kind))
	}
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

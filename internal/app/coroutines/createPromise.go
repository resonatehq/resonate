package coroutines

import (
	"fmt"
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/message"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/task"
)

func CreatePromise(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	util.Assert(r.Kind == t_api.CreatePromise, "must be create promise")

	return createPromiseAndTask(c, r, r.CreatePromise, nil)
}

func CreatePromiseAndTask(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	util.Assert(r.Kind == t_api.CreatePromiseAndTask, "must be create promise and task")
	util.Assert(r.CreatePromiseAndTask.Promise.Id == r.CreatePromiseAndTask.Task.PromiseId, "promise ids must match")
	util.Assert(r.CreatePromiseAndTask.Promise.Timeout == r.CreatePromiseAndTask.Task.Timeout, "timeouts must match")

	return createPromiseAndTask(c, r, r.CreatePromiseAndTask.Promise, &t_aio.CreateTaskCommand{
		Id:        invokeTaskId(r.CreatePromiseAndTask.Task.PromiseId),
		Recv:      nil,
		Mesg:      &message.Mesg{Type: message.Invoke, Root: r.CreatePromiseAndTask.Task.PromiseId, Leaf: r.CreatePromiseAndTask.Task.PromiseId},
		Timeout:   r.CreatePromiseAndTask.Task.Timeout,
		ProcessId: &r.CreatePromiseAndTask.Task.ProcessId,
		State:     task.Claimed,
		Ttl:       r.CreatePromiseAndTask.Task.Ttl,
		ExpiresAt: c.Time() + int64(r.CreatePromiseAndTask.Task.Ttl),
		CreatedOn: c.Time(),
	})
}

func createPromiseAndTask(
	c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any],
	r *t_api.Request,
	createPromiseReq *t_api.CreatePromiseRequest,
	taskCmd *t_aio.CreateTaskCommand,
) (*t_api.Response, error) {
	util.Assert(r.Kind == t_api.CreatePromise || r.Kind == t_api.CreatePromiseAndTask, "must be create promise or variant")

	// response status
	var status t_api.StatusCode

	// response data
	var p *promise.Promise
	var t *task.Task

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
		promiseCmd := &t_aio.CreatePromiseCommand{
			Id:             createPromiseReq.Id,
			Param:          createPromiseReq.Param,
			Timeout:        createPromiseReq.Timeout,
			IdempotencyKey: createPromiseReq.IdempotencyKey,
			Tags:           createPromiseReq.Tags,
			CreatedOn:      c.Time(),
		}

		// if the promise does not exist, create it
		completion, err := gocoro.SpawnAndAwait(c, createPromise(r.Tags, promiseCmd, taskCmd))
		if err != nil {
			return nil, err
		}

		if completion.Store.Results[0].CreatePromise.RowsAffected == 0 {
			// It's possible that the promise was created by another coroutine
			// while we were creating. In that case, we should just retry.
			return createPromiseAndTask(c, r, createPromiseReq, taskCmd)
		}

		// set status
		status = t_api.StatusCreated

		// set promise
		p = &promise.Promise{
			Id:                      promiseCmd.Id,
			State:                   promise.Pending,
			Param:                   promiseCmd.Param,
			Timeout:                 promiseCmd.Timeout,
			IdempotencyKeyForCreate: promiseCmd.IdempotencyKey,
			Tags:                    promiseCmd.Tags,
			CreatedOn:               &promiseCmd.CreatedOn,
		}

		switch r.Kind {
		case t_api.CreatePromiseAndTask:
			util.Assert(taskCmd != nil, "create task cmd must not be nil")
			util.Assert(completion.Store.Results[1].Kind == t_aio.CreateTask, "completion must be create task")

			t = &task.Task{
				Id:            taskCmd.Id,
				ProcessId:     taskCmd.ProcessId,
				RootPromiseId: p.Id,
				State:         taskCmd.State,
				Recv:          taskCmd.Recv,
				Mesg:          taskCmd.Mesg,
				Timeout:       taskCmd.Timeout,
				Counter:       1,
				Attempt:       0,
				Ttl:           taskCmd.Ttl,
				ExpiresAt:     taskCmd.ExpiresAt,
				CreatedOn:     &taskCmd.CreatedOn,
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
				Id:             createPromiseReq.Id,
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
				return createPromiseAndTask(c, r, createPromiseReq, taskCmd)
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
		} else if !(createPromiseReq.Strict && p.State != promise.Pending) && p.IdempotencyKeyForCreate.Match(createPromiseReq.IdempotencyKey) {
			// switch status to ok if not strict and idempotency keys match
			status = t_api.StatusOK
		} else {
			status = t_api.StatusPromiseAlreadyExists
		}
	}

	res := &t_api.Response{Kind: r.Kind, Tags: r.Tags}

	switch r.Kind {
	case t_api.CreatePromise:
		res.CreatePromise = &t_api.CreatePromiseResponse{Status: status, Promise: p}
	case t_api.CreatePromiseAndTask:
		res.CreatePromiseAndTask = &t_api.CreatePromiseAndTaskResponse{Status: status, Promise: p, Task: t}
	}

	return res, nil
}

func createPromise(tags map[string]string, promiseCmd *t_aio.CreatePromiseCommand, taskCmd *t_aio.CreateTaskCommand, additionalCmds ...*t_aio.Command) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, *t_aio.Completion] {
	if promiseCmd.Param.Headers == nil {
		promiseCmd.Param.Headers = map[string]string{}
	}
	if promiseCmd.Param.Data == nil {
		promiseCmd.Param.Data = []byte{}
	}
	if promiseCmd.Tags == nil {
		promiseCmd.Tags = map[string]string{}
	}

	return func(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, *t_aio.Completion]) (*t_aio.Completion, error) {
		// add create promise command
		commands := []*t_aio.Command{{
			Kind:          t_aio.CreatePromise,
			CreatePromise: promiseCmd,
		}}

		// check router to see if a task needs to be created
		completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
			Kind: t_aio.Router,
			Tags: tags,
			Router: &t_aio.RouterSubmission{
				Promise: &promise.Promise{
					Id:                      promiseCmd.Id,
					State:                   promise.Pending,
					Param:                   promiseCmd.Param,
					Timeout:                 promiseCmd.Timeout,
					IdempotencyKeyForCreate: promiseCmd.IdempotencyKey,
					Tags:                    promiseCmd.Tags,
					CreatedOn:               &promiseCmd.CreatedOn,
				},
			},
		})

		if err != nil {
			slog.Warn("failed to match promise", "cmd", promiseCmd, "err", err)
		}

		if taskCmd != nil && (err != nil || !completion.Router.Matched) {
			slog.Error("failed to match promise when creating a task", "cmd", promiseCmd)
			return nil, t_api.NewError(t_api.StatusPromiseRecvNotFound, err)
		}

		if err == nil && completion.Router.Matched {
			util.Assert(completion.Router.Recv != nil, "recv must not be nil")

			// If there is a taskCmd just update the Recv otherwise create a tasks for the match
			if taskCmd != nil {
				taskCmd.Recv = completion.Router.Recv
			} else {
				taskCmd = &t_aio.CreateTaskCommand{
					Id:        invokeTaskId(promiseCmd.Id),
					Recv:      completion.Router.Recv,
					Mesg:      &message.Mesg{Type: message.Invoke, Root: promiseCmd.Id, Leaf: promiseCmd.Id},
					Timeout:   promiseCmd.Timeout,
					State:     task.Init,
					CreatedOn: promiseCmd.CreatedOn,
				}

			}

			// add create task command if matched
			commands = append(commands, &t_aio.Command{
				Kind:       t_aio.CreateTask,
				CreateTask: taskCmd,
			})
		}

		// add additional commands
		commands = append(commands, additionalCmds...)

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

func invokeTaskId(promiseId string) string {
	return fmt.Sprintf("__invoke:%s", promiseId)
}

package coroutines

import (
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
	req := r.Payload.(*t_api.CreatePromiseRequest)
	return createPromiseAndTask(c, r, req, nil)
}

func CreatePromiseAndTask(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	req := r.Payload.(*t_api.CreatePromiseAndTaskRequest)
	util.Assert(req.Promise.Id == req.Task.PromiseId, "promise ids must match")
	util.Assert(req.Promise.Timeout == req.Task.Timeout, "timeouts must match")

	return createPromiseAndTask(c, r, req.Promise, &t_aio.CreateTaskCommand{
		Id:        util.InvokeId(req.Task.PromiseId),
		Recv:      nil,
		Mesg:      &message.Mesg{Type: message.Invoke, Root: req.Task.PromiseId, Leaf: req.Task.PromiseId},
		Timeout:   req.Task.Timeout,
		ProcessId: &req.Task.ProcessId,
		State:     task.Claimed,
		Ttl:       req.Task.Ttl,
		ExpiresAt: c.Time() + int64(req.Task.Ttl),
		CreatedOn: c.Time(),
	})
}

func createPromiseAndTask(
	c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any],
	r *t_api.Request,
	req *t_api.CreatePromiseRequest,
	taskCmd *t_aio.CreateTaskCommand,
) (*t_api.Response, error) {
	util.Assert(r.Kind() == t_api.CreatePromise || r.Kind() == t_api.CreatePromiseAndTask, "must be create promise or variant")

	// response status
	var status t_api.StatusCode

	// response data
	var p *promise.Promise
	var t *task.Task

	// first read the promise to see if it already exists
	completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
		Kind: t_aio.Store,
		Tags: r.Metadata,
		Store: &t_aio.StoreSubmission{
			Transaction: &t_aio.Transaction{
				Commands: []t_aio.Command{
					&t_aio.ReadPromiseCommand{
						Id: req.Id,
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

	result := t_aio.AsQueryPromises(completion.Store.Results[0])
	util.Assert(result.RowsReturned == 0 || result.RowsReturned == 1, "result must return 0 or 1 rows")

	if result.RowsReturned == 0 {
		promiseCmd := &t_aio.CreatePromiseCommand{
			Id:             req.Id,
			Param:          req.Param,
			Timeout:        req.Timeout,
			IdempotencyKey: req.IdempotencyKey,
			Tags:           req.Tags,
			CreatedOn:      c.Time(),
		}

		// if the promise does not exist, create it
		completion, err := gocoro.SpawnAndAwait(c, createPromise(r.Metadata, promiseCmd, taskCmd))
		if err != nil {
			return nil, err
		}
		var promiseRowsAffected int64

		switch v := completion.Store.Results[0].(type) {
		case *t_aio.AlterPromisesResult:
			promiseRowsAffected = v.RowsAffected
		case *t_aio.AlterPromisesAndTasksResult:
			promiseRowsAffected = v.PromiseRowsAffected
			util.Assert(promiseRowsAffected == v.TaskRowsAffected, "number of promises and tasks affected must be equal.")
		default:
			panic("invalid type.")
		}

		if promiseRowsAffected == 0 {
			// It's possible that the promise was created by another coroutine
			// while we were creating. In that case, we should just retry.
			return createPromiseAndTask(c, r, req, taskCmd)
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

		if r.Kind() == t_api.CreatePromiseAndTask {
			util.Assert(taskCmd != nil, "create task cmd must not be nil")
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
				Id:             req.Id,
				State:          promise.GetTimedoutState(p),
				Value:          promise.Value{},
				IdempotencyKey: nil,
				CompletedOn:    p.Timeout,
			}

			ok, err := gocoro.SpawnAndAwait(c, completePromise(r.Metadata, cmd))
			if err != nil {
				return nil, err
			}

			if !ok {
				// It's possible that the promise was created by another coroutine
				// while we were timing out. In that case, we should just retry.
				return createPromiseAndTask(c, r, req, taskCmd)
			}

			// set status to ok if not strict and idempotency keys match
			if !req.Strict && p.IdempotencyKeyForCreate.Match(req.IdempotencyKey) {
				status = t_api.StatusOK
			} else {
				status = t_api.StatusPromiseAlreadyExists
			}

			// update promise
			p.State = cmd.State
			p.Value = cmd.Value
			p.IdempotencyKeyForComplete = cmd.IdempotencyKey
			p.CompletedOn = &cmd.CompletedOn
		} else if (!req.Strict || p.State == promise.Pending) && p.IdempotencyKeyForCreate.Match(req.IdempotencyKey) {
			// switch status to ok if not strict and idempotency keys match
			status = t_api.StatusOK
		} else {
			status = t_api.StatusPromiseAlreadyExists
		}
	}

	res := &t_api.Response{Status: status, Metadata: r.Metadata}

	switch r.Kind() {
	case t_api.CreatePromise:
		res.Payload = &t_api.CreatePromiseResponse{Promise: p}
	case t_api.CreatePromiseAndTask:
		res.Payload = &t_api.CreatePromiseAndTaskResponse{Promise: p, Task: t}
	}

	return res, nil
}

func createPromise(tags map[string]string, promiseCmd *t_aio.CreatePromiseCommand, taskCmd *t_aio.CreateTaskCommand, additionalCmds ...t_aio.Command) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, *t_aio.Completion] {
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
		commands := []t_aio.Command{}

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
			slog.Error("failed to match promise with router when creating a task", "cmd", promiseCmd)
			return nil, t_api.NewError(t_api.StatusPromiseRecvNotFound, err)
		}

		var cmd t_aio.Command = promiseCmd

		if err == nil && completion.Router.Matched {
			util.Assert(completion.Router.Recv != nil, "recv must not be nil")

			// If there is a taskCmd just update the Recv otherwise create a tasks for the match
			if taskCmd != nil {
				// Note: we are mutating the taskCmd that is already merged with the createPromiseCmd
				taskCmd.Recv = completion.Router.Recv
			} else {
				taskCmd = &t_aio.CreateTaskCommand{
					Id:        util.InvokeId(promiseCmd.Id),
					Recv:      completion.Router.Recv,
					Mesg:      &message.Mesg{Type: message.Invoke, Root: promiseCmd.Id, Leaf: promiseCmd.Id},
					Timeout:   promiseCmd.Timeout,
					State:     task.Init,
					CreatedOn: promiseCmd.CreatedOn,
				}
			}

			cmd = &t_aio.CreatePromiseAndTaskCommand{
				PromiseCommand: promiseCmd,
				TaskCommand:    taskCmd,
			}
		}

		// Add the main command
		commands = append(commands, cmd)
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

		switch v := completion.Store.Results[0].(type) {
		case *t_aio.AlterPromisesAndTasksResult:
			util.Assert(v.PromiseRowsAffected == 0 || v.PromiseRowsAffected == 1, "Creating promise result must return 0 or 1 rows")
			util.Assert(v.TaskRowsAffected == v.PromiseRowsAffected, "If not promise was created a task must have not been created")
		case *t_aio.AlterPromisesResult:
			util.Assert(v.RowsAffected == 0 || v.RowsAffected == 1, "CreatePromise result must return 0 or 1 rows")
		default:
			panic("First result must be CreatePromise or CreatePromiseAndTask")
		}

		return completion, nil
	}
}

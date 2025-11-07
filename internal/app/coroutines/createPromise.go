package coroutines

import (
	"errors"
	"log/slog"
	"strconv"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/message"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/task"
)

func CreatePromise(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	req := r.Payload.(*t_api.CreatePromiseRequest)

	cmd := &t_aio.CreatePromiseCommand{
		Id:             req.Id,
		Param:          req.Param,
		Timeout:        req.Timeout,
		IdempotencyKey: req.IdempotencyKey,
		Tags:           req.Tags,
		CreatedOn:      c.Time(),
	}

	completion, err := gocoro.SpawnAndAwait(c, createPromise(r.Metadata, r.Fence, cmd, nil))

	if err != nil {
		return nil, err
	}

	var status t_api.StatusCode

	if completion.created {
		status = t_api.StatusCreated
	} else if (!req.Strict || completion.promise.State == promise.Pending) && completion.promise.IdempotencyKeyForCreate.Match(req.IdempotencyKey) {
		status = t_api.StatusOK
	} else {
		status = t_api.StatusPromiseAlreadyExists
	}

	return &t_api.Response{
		Status:   status,
		Metadata: r.Metadata,
		Payload:  &t_api.CreatePromiseResponse{Promise: completion.promise},
	}, nil
}

func CreatePromiseAndTask(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	req := r.Payload.(*t_api.CreatePromiseAndTaskRequest)

	cmd := &t_aio.CreatePromiseCommand{
		Id:             req.Promise.Id,
		Param:          req.Promise.Param,
		Timeout:        req.Promise.Timeout,
		IdempotencyKey: req.Promise.IdempotencyKey,
		Tags:           req.Promise.Tags,
		CreatedOn:      c.Time(),
	}

	head := map[string]string{}
	if traceparent, ok := r.Metadata["traceparent"]; ok {
		head["traceparent"] = traceparent
	}
	if tracestate, ok := r.Metadata["tracestate"]; ok {
		head["tracestate"] = tracestate
	}

	completion, err := gocoro.SpawnAndAwait(c, createPromise(r.Metadata, nil, cmd, &t_aio.CreateTaskCommand{
		Id:        util.InvokeId(req.Task.PromiseId),
		Recv:      nil,
		Mesg:      &message.Mesg{Type: message.Invoke, Head: head, Root: req.Task.PromiseId, Leaf: req.Task.PromiseId},
		Timeout:   req.Task.Timeout,
		ProcessId: &req.Task.ProcessId,
		State:     task.Claimed,
		Ttl:       req.Task.Ttl,
		ExpiresAt: c.Time() + int64(req.Task.Ttl),
		CreatedOn: c.Time(),
	}))

	if err != nil {
		return nil, err
	}

	var status t_api.StatusCode

	if completion.created {
		status = t_api.StatusCreated
	} else if (!req.Promise.Strict || completion.promise.State == promise.Pending) && completion.promise.IdempotencyKeyForCreate.Match(req.Promise.IdempotencyKey) {
		status = t_api.StatusOK
	} else {
		status = t_api.StatusPromiseAlreadyExists
	}

	return &t_api.Response{
		Status:   status,
		Metadata: r.Metadata,
		Payload:  &t_api.CreatePromiseAndTaskResponse{Promise: completion.promise, Task: completion.task},
	}, nil
}

type promiseAndTask struct {
	created bool
	promise *promise.Promise
	task    *task.Task
}

func createPromise(tags map[string]string, fence *task.FencingToken, promiseCmd *t_aio.CreatePromiseCommand, taskCmd *t_aio.CreateTaskCommand, additionalCmds ...t_aio.Command) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, *promiseAndTask] {
	if promiseCmd.Param.Headers == nil {
		promiseCmd.Param.Headers = map[string]string{}
	}
	if promiseCmd.Param.Data == nil {
		promiseCmd.Param.Data = []byte{}
	}
	if promiseCmd.Tags == nil {
		promiseCmd.Tags = map[string]string{}
	}

	return func(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, *promiseAndTask]) (*promiseAndTask, error) {
		metrics, ok := c.Get("metrics").(*metrics.Metrics)
		util.Assert(ok, "coroutine must have metrics dependency")

		// first read the promise to see if it already exists
		completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
			Kind: t_aio.Store,
			Tags: tags,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Fence: fence,
					Commands: []t_aio.Command{
						&t_aio.ReadPromiseCommand{
							Id: promiseCmd.Id,
						},
					},
				},
			},
		})

		if err != nil {
			slog.Error("failed to read promise", "cmd", promiseCmd, "err", err)
			return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
		}

		if !completion.Store.Valid {
			return nil, t_api.NewError(t_api.StatusTaskPreconditionFailed, errors.New("the specified task is not valid"))
		}

		util.Assert(completion.Store != nil, "completion must not be nil")

		result := t_aio.AsQueryPromises(completion.Store.Results[0])
		util.Assert(result.RowsReturned == 0 || result.RowsReturned == 1, "result must return 0 or 1 rows")

		// response data
		var p *promise.Promise
		var t *task.Task

		if result.RowsReturned == 0 {
			commands := []t_aio.Command{}

			p = &promise.Promise{
				Id:                      promiseCmd.Id,
				State:                   promise.Pending,
				Param:                   promiseCmd.Param,
				Timeout:                 promiseCmd.Timeout,
				IdempotencyKeyForCreate: promiseCmd.IdempotencyKey,
				Tags:                    promiseCmd.Tags,
				CreatedOn:               &promiseCmd.CreatedOn,
			}

			// first, check the router to see if a task needs to be created
			completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
				Kind: t_aio.Router,
				Tags: tags,
				Router: &t_aio.RouterSubmission{
					Promise: p,
				},
			})

			if err != nil {
				slog.Warn("failed to match promise", "cmd", promiseCmd, "err", err)
			}

			if err == nil && completion.Router.Matched {
				util.Assert(completion.Router.Recv != nil, "recv must not be nil")

				if taskCmd != nil {
					// just set the recv
					taskCmd.Recv = completion.Router.Recv
				} else {
					var expiresAt int64
					if delay, ok := promiseCmd.Tags["resonate:delay"]; ok {
						expiresAt, err = strconv.ParseInt(delay, 10, 64)
						if err != nil {
							slog.Warn("could not parse delay", "promise", promiseCmd.Id, "delay", delay)
						}
					}

					head := map[string]string{}
					if traceparent, ok := tags["traceparent"]; ok {
						head["traceparent"] = traceparent
					}
					if tracestate, ok := tags["tracestate"]; ok {
						head["tracestate"] = tracestate
					}

					// add the task command
					taskCmd = &t_aio.CreateTaskCommand{
						Id:        util.InvokeId(promiseCmd.Id),
						Recv:      completion.Router.Recv,
						Mesg:      &message.Mesg{Type: message.Invoke, Head: head, Root: promiseCmd.Id, Leaf: promiseCmd.Id},
						Timeout:   promiseCmd.Timeout,
						State:     task.Init,
						ExpiresAt: expiresAt,
						CreatedOn: promiseCmd.CreatedOn,
					}
				}

				// add a create promise and task command
				commands = append(commands, &t_aio.CreatePromiseAndTaskCommand{
					PromiseCommand: promiseCmd,
					TaskCommand:    taskCmd,
				})

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
			} else {
				// add create promise command
				commands = append(commands, promiseCmd)
			}

			completion, err = gocoro.YieldAndAwait(c, &t_aio.Submission{
				Kind: t_aio.Store,
				Tags: tags,
				Store: &t_aio.StoreSubmission{
					Transaction: &t_aio.Transaction{
						Fence:    fence,
						Commands: append(commands, additionalCmds...),
					},
				},
			})

			if err != nil {
				slog.Error("failed to create promise", "err", err)
				return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
			}

			if !completion.Store.Valid {
				return nil, t_api.NewError(t_api.StatusTaskPreconditionFailed, errors.New("the specified task is not valid"))
			}

			util.Assert(completion.Store != nil, "completion must not be nil")
			util.Assert(len(completion.Store.Results) == len(commands)+len(additionalCmds), "completion must have same number of results as commands")
			result := t_aio.AsAlterPromises(completion.Store.Results[0])

			if result.RowsAffected == 0 {
				// It's possible that the promise was created by another coroutine
				// while we were creating. In that case, we should just retry.
				return gocoro.SpawnAndAwait(c, createPromise(tags, fence, promiseCmd, taskCmd, additionalCmds...))
			}

			// count promise
			metrics.Promises.WithLabelValues("created").Inc()

			// count task (if applicable)
			if t != nil {
				metrics.Tasks.WithLabelValues("created").Inc()
			}

			return &promiseAndTask{created: true, promise: p, task: t}, nil
		}

		p, err = result.Records[0].Promise()
		if err != nil {
			slog.Error("failed to parse promise record", "record", result.Records[0], "err", err)
			return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
		}

		if p.State == promise.Pending && p.Timeout <= c.Time() {
			ok, err := gocoro.SpawnAndAwait(c, completePromise(tags, fence, &t_aio.UpdatePromiseCommand{
				Id:             promiseCmd.Id,
				State:          promise.GetTimedoutState(p),
				Value:          promise.Value{},
				IdempotencyKey: nil,
				CompletedOn:    p.Timeout,
			}))
			if err != nil {
				return nil, err
			}

			if !ok {
				// It's possible that the promise was created by another coroutine
				// while we were creating. In that case, we should just retry.
				return gocoro.SpawnAndAwait(c, createPromise(tags, fence, promiseCmd, taskCmd, additionalCmds...))
			}

			// update promise
			p.State = promise.GetTimedoutState(p)
			p.Value = promise.Value{}
			p.IdempotencyKeyForComplete = nil
			p.CompletedOn = &p.Timeout
		}

		return &promiseAndTask{created: false, promise: p, task: t}, nil
	}
}

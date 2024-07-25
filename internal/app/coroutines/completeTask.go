package coroutines

import (
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/task"
)

func CompleteTask(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	// Read task.
	rc, err := readTask(c, r, r.CompleteTask.TaskId)
	if err != nil {
		return nil, err
	}

	util.Assert(rc.Store != nil, "completion must not be nil")
	readTaskResult := rc.Store.Results[0].ReadTask
	util.Assert(readTaskResult.RowsReturned == 0 || readTaskResult.RowsReturned == 1, "result must return 0 or 1 rows")

	if readTaskResult.RowsReturned == 0 {
		return &t_api.Response{
			Kind: t_api.CompleteTask,
			Tags: r.Tags,
			CompleteTask: &t_api.CompleteTaskResponse{
				Status: t_api.StatusTaskNotFound,
			},
		}, nil
	}

	task, err := readTaskResult.Records[0].Task()
	if err != nil {
		slog.Error("failed to parse task record", "record", readTaskResult.Records[0], "err", err)
		return nil, t_api.NewResonateError(t_api.ErrAIOStoreSerializationFailure, "failed to parse task record", err)
	}

	// Validate the complete request is valid and task is completeable.
	isCompleteable := isCompleteableTask(c, r, task)
	if isCompleteable != nil {
		return isCompleteable, nil
	}

	// Complete the task.
	wc, err := completeTask(c, r, task)
	if err != nil {
		return nil, err
	}

	// Assert store.
	util.Assert(wc.Store != nil, "completion must not be nil")

	// Assert lock write.
	releaseLockResult := wc.Store.Results[0].ReleaseLock
	util.Assert(releaseLockResult.RowsAffected == 0 || releaseLockResult.RowsAffected == 1, "result must return 0 or 1 rows")

	// Assert task write.
	updateTaskResult := wc.Store.Results[1].UpdateTask
	util.Assert(updateTaskResult.RowsAffected == 1, "result must return 1 row")

	// Assert promise write.
	promiseResult := wc.Store.Results[2].UpdatePromise
	util.Assert(promiseResult.RowsAffected == 1, "result must return 1 row")

	return &t_api.Response{
		Kind: t_api.CompleteTask,
		CompleteTask: &t_api.CompleteTaskResponse{
			Status: t_api.StatusOK,
		},
	}, nil
}

func isCompleteableTask(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], req *t_api.Request, task *task.Task) *t_api.Response {
	var resp *t_api.Response

	// Can't complete a task that is already completed.
	if task.IsCompleted {
		resp = &t_api.Response{
			Kind: t_api.CompleteTask,
			Tags: req.Tags,
			CompleteTask: &t_api.CompleteTaskResponse{
				Status: t_api.StatusTaskAlreadyCompleted,
			},
		}
	}

	// Can't complete a task with an outdated counter in the request.
	if task.Counter != req.CompleteTask.Counter {
		resp = &t_api.Response{
			Kind: t_api.CompleteTask,
			Tags: req.Tags,
			CompleteTask: &t_api.CompleteTaskResponse{
				Status: t_api.StatusTaskWrongCounter,
			},
		}
	}

	// Can't complete a task that is already timedout. Must be enqued again by enqueueTask coroutine.
	if task.CompleteTimeout < c.Time() {
		resp = &t_api.Response{
			Kind: t_api.CompleteTask,
			Tags: req.Tags,
			CompleteTask: &t_api.CompleteTaskResponse{
				Status: t_api.StatusTaskAlreadyTimedOut,
			},
		}
	}

	// Can't complete a task that is already globally timedout. No work can be done on it. todo: retries...?
	if task.PromiseTimeout < c.Time() {
		resp = &t_api.Response{
			Kind: t_api.CompleteTask,
			Tags: req.Tags,
			CompleteTask: &t_api.CompleteTaskResponse{
				Status: t_api.StatusTaskAlreadyTimedOut,
			},
		}
	}

	return resp
}

func completeTask(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], req *t_api.Request, task *task.Task) (*t_aio.Completion, error) {
	completedOn := c.Time()

	completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
		Kind: t_aio.Store,
		Tags: req.Tags,
		Store: &t_aio.StoreSubmission{
			Transaction: &t_aio.Transaction{
				Commands: []*t_aio.Command{
					{
						Kind: t_aio.ReleaseLock,
						ReleaseLock: &t_aio.ReleaseLockCommand{
							ResourceId:  task.PromiseId,
							ExecutionId: req.CompleteTask.ExecutionId,
						},
					},
					{
						Kind: t_aio.UpdateTask,
						UpdateTask: &t_aio.UpdateTaskCommand{
							Id:              task.Id,
							Counter:         task.Counter,
							ClaimTimeout:    task.ClaimTimeout,
							CompleteTimeout: task.CompleteTimeout,
							CompletedOn:     completedOn,
							IsCompleted:     true,
						},
					},
					{
						Kind: t_aio.UpdatePromise,
						UpdatePromise: &t_aio.UpdatePromiseCommand{
							Id:          task.PromiseId,
							State:       req.CompleteTask.State,
							Value:       req.CompleteTask.Value,
							CompletedOn: completedOn,
						},
					},
				},
			},
		},
	})
	if err != nil {
		return nil, t_api.NewResonateError(t_api.ErrAIOStoreFailure, "failed to complete task", err)
	}

	return completion, nil
}

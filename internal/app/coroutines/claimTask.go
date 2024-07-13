package coroutines

import (
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/task"
)

func ClaimTask(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	// Read task.
	rc, err := readTask(c, r, r.ClaimTask.TaskId)
	if err != nil {
		return nil, err
	}

	util.Assert(rc.Store != nil, "completion must not be nil")
	readTaskResult := rc.Store.Results[0].ReadTask
	util.Assert(readTaskResult.RowsReturned == 0 || readTaskResult.RowsReturned == 1, "result must return 0 or 1 rows")

	if readTaskResult.RowsReturned == 0 {
		return &t_api.Response{
			Kind: t_api.ClaimTask,
			Tags: r.Tags,
			ClaimTask: &t_api.ClaimTaskResponse{
				Status: t_api.StatusTaskNotFound,
			},
		}, nil
	}

	task, err := readTaskResult.Records[0].Task()
	if err != nil {
		slog.Error("failed to parse task record", "record", readTaskResult.Records[0], "err", err)
		return nil, t_api.NewResonateError(t_api.ErrAIOStoreSerializationFailure, "failed to parse task record", err)
	}

	// Validate the claim request is valid and task is claimable
	isClaimable := isClaimableTask(c, r, task)
	if isClaimable != nil {
		return isClaimable, nil
	}

	// Try Claim Task. Task is claimed if lock is acquired.
	wc, err := tryClaimTask(c, r, task)
	if err != nil {
		return nil, err
	}

	util.Assert(wc.Store != nil, "completion must not be nil")

	// Check lock write.
	lockRes := wc.Store.Results[0].AcquireLock
	util.Assert(lockRes.RowsAffected == 0 || lockRes.RowsAffected == 1, "result must return 0 or 1 rows")

	if lockRes.RowsAffected == 0 {
		return &t_api.Response{
			Kind: t_api.ClaimTask,
			Tags: r.Tags,
			ClaimTask: &t_api.ClaimTaskResponse{
				Status: t_api.StatusLockAlreadyAcquired,
			},
		}, nil
	}

	// Check task write.
	updateTaskResult := wc.Store.Results[1].UpdateTask
	util.Assert(updateTaskResult.RowsAffected == 1, "result must return 1 row")

	// Check promise read.
	promiseResult := wc.Store.Results[2].ReadPromise
	util.Assert(promiseResult.RowsReturned == 1, "result must return 1 row")

	// Respond.
	promise, err := promiseResult.Records[0].Promise()
	if err != nil {
		slog.Error("failed to parse promise record", "record", promiseResult.Records[0], "err", err)
		return nil, t_api.NewResonateError(t_api.ErrAIOStoreSerializationFailure, "failed to parse promise record", err)
	}

	return &t_api.Response{
		Kind: t_api.ClaimTask,
		Tags: r.Tags,
		ClaimTask: &t_api.ClaimTaskResponse{
			Status:  t_api.StatusOK,
			Promise: promise,
		},
	}, nil
}

func readTask(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], req *t_api.Request, taskId string) (*t_aio.Completion, error) {
	completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
		Kind: t_aio.Store,
		Tags: req.Tags,
		Store: &t_aio.StoreSubmission{
			Transaction: &t_aio.Transaction{
				Commands: []*t_aio.Command{
					{
						Kind: t_aio.ReadTask,
						ReadTask: &t_aio.ReadTaskCommand{
							Id: taskId,
						},
					},
				},
			},
		},
	})
	if err != nil {
		slog.Error("failed to get task", "req", req, "err", err)
		return nil, t_api.NewResonateError(t_api.ErrAIOStoreFailure, "failed to get task", err)
	}

	return completion, nil
}

func isClaimableTask(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], req *t_api.Request, task *task.Task) *t_api.Response {
	var resp *t_api.Response

	// Can't claim a task that is already completed.
	if task.IsCompleted {
		resp = &t_api.Response{
			Kind: t_api.ClaimTask,
			Tags: req.Tags,
			ClaimTask: &t_api.ClaimTaskResponse{
				Status: t_api.StatusTaskAlreadyCompleted,
			},
		}
	}

	// Can't claim a task with an outdated counter in the request.
	if task.Counter != req.ClaimTask.Counter {
		resp = &t_api.Response{
			Kind: t_api.ClaimTask,
			Tags: req.Tags,
			ClaimTask: &t_api.ClaimTaskResponse{
				Status: t_api.StatusTaskWrongCounter,
			},
		}
	}

	// Can't claim a task that is already timedout. Must be enqued again by enqueueTask coroutine.
	if task.ClaimTimeout < c.Time() {
		resp = &t_api.Response{
			Kind: t_api.ClaimTask,
			Tags: req.Tags,
			ClaimTask: &t_api.ClaimTaskResponse{
				Status: t_api.StatusTaskAlreadyTimedOut,
			},
		}
	}

	// Can't claim a task that is already globally timedout. No work can be done on it. todo: retries...?
	if task.PromiseTimeout < c.Time() {
		resp = &t_api.Response{
			Kind: t_api.ClaimTask,
			Tags: req.Tags,
			ClaimTask: &t_api.ClaimTaskResponse{
				Status: t_api.StatusTaskAlreadyTimedOut,
			},
		}
	}

	return resp
}

func tryClaimTask(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], req *t_api.Request, task *task.Task) (*t_aio.Completion, error) {
	timeout := c.Time() + req.ClaimTask.ExpiryInMilliseconds

	// Optimizing for the happy path. JUST GO FOR IT and avoid two writes.
	completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
		Kind: t_aio.Store,
		Tags: req.Tags,
		Store: &t_aio.StoreSubmission{
			Transaction: &t_aio.Transaction{
				Commands: []*t_aio.Command{
					{
						Kind: t_aio.AcquireLock,
						AcquireLock: &t_aio.AcquireLockCommand{
							ResourceId:           task.PromiseId,
							ProcessId:            req.ClaimTask.ProcessId,
							ExecutionId:          req.ClaimTask.ExecutionId,
							ExpiryInMilliseconds: req.ClaimTask.ExpiryInMilliseconds,
							Timeout:              timeout, // from s to ms
						},
					},
					{
						Kind: t_aio.UpdateTask,
						UpdateTask: &t_aio.UpdateTaskCommand{
							Id:              task.Id,
							Counter:         task.Counter,
							ClaimTimeout:    task.ClaimTimeout,
							CompleteTimeout: timeout,
							CompletedOn:     task.CompletedOn,
							IsCompleted:     task.IsCompleted,
						},
					},
					{
						Kind: t_aio.ReadPromise,
						ReadPromise: &t_aio.ReadPromiseCommand{
							Id: task.PromiseId,
						},
					},
				},
			},
		},
	})
	if err != nil {
		return nil, t_api.NewResonateError(t_api.ErrAIOStoreFailure, "failed to acquire lock", err)
	}

	return completion, nil
}

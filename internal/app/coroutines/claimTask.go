package coroutines

import (
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/metadata"
	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/task"
)

func ClaimTask(metadata *metadata.Metadata, req *t_api.Request, res CallBackFn) *Coroutine {
	return scheduler.NewCoroutine(metadata, func(c *Coroutine) {
		// Read task.
		rc, err := readTask(c, req, req.ClaimTask.TaskId)
		if err != nil {
			res(nil, err)
			return
		}

		util.Assert(rc.Store != nil, "completion must not be nil")
		readTaskResult := rc.Store.Results[0].ReadTask
		util.Assert(readTaskResult.RowsReturned == 0 || readTaskResult.RowsReturned == 1, "result must return 0 or 1 rows")

		if readTaskResult.RowsReturned == 0 {
			res(&t_api.Response{
				Kind: t_api.ClaimTask,
				ClaimTask: &t_api.ClaimTaskResponse{
					Status: t_api.StatusTaskNotFound,
				},
			}, nil)
			return
		}

		task, err := readTaskResult.Records[0].Task()
		if err != nil {
			slog.Error("failed to parse task record", "record", readTaskResult.Records[0], "err", err)
			res(nil, t_api.NewResonateError(t_api.ErrAIOStoreSerializationFailure, "failed to parse task record", err))
			return
		}

		// Validate the claim request is valid and task is claimable.
		isClaimable := isClaimableTask(c, req, task)
		if isClaimable != nil {
			res(isClaimable, nil)
			return
		}

		// Try Claim Task. Task is claimed if lock is acquired.
		wc, err := tryClaimTask(c, req, task)
		if err != nil {
			res(nil, err)
			return
		}

		util.Assert(wc.Store != nil, "completion must not be nil")

		// check lock write.
		lockRes := wc.Store.Results[0].AcquireLock
		util.Assert(lockRes.RowsAffected == 0 || lockRes.RowsAffected == 1, "result must return 0 or 1 rows")

		if lockRes.RowsAffected == 0 {
			res(&t_api.Response{
				Kind: t_api.ClaimTask,
				ClaimTask: &t_api.ClaimTaskResponse{
					Status: t_api.StatusLockAlreadyAcquired,
				},
			}, nil)
			return
		}

		// check task write.
		updateTaskResult := wc.Store.Results[1].UpdateTask
		util.Assert(updateTaskResult.RowsAffected == 1, "result must return 1 row")

		// check promise read.
		promiseResult := wc.Store.Results[2].ReadPromise
		util.Assert(promiseResult.RowsReturned == 1, "result must return 1 row")

		// Respond.
		promise, err := promiseResult.Records[0].Promise()
		if err != nil {
			slog.Error("failed to parse promise record", "record", promiseResult.Records[0], "err", err)
			res(nil, t_api.NewResonateError(t_api.ErrAIOStoreSerializationFailure, "failed to parse promise record", err))
			return
		}

		res(&t_api.Response{
			Kind: t_api.ClaimTask,
			ClaimTask: &t_api.ClaimTaskResponse{
				Status:  t_api.StatusOK,
				Promise: promise,
			},
		}, nil)
	})
}

func readTask(c *Coroutine, req *t_api.Request, taskId string) (*t_aio.Completion, error) {
	completion, err := c.Yield(&t_aio.Submission{
		Kind: t_aio.Store,
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

func isClaimableTask(c *Coroutine, req *t_api.Request, task *task.Task) *t_api.Response {
	var resp *t_api.Response

	// Can't claim a task that is already completed.
	if task.IsCompleted {
		resp = &t_api.Response{
			Kind: t_api.ClaimTask,
			ClaimTask: &t_api.ClaimTaskResponse{
				Status: t_api.StatusTaskAlreadyCompleted,
			},
		}
	}

	// Can't claim a task with an outdated counter in the request.
	if task.Counter != req.ClaimTask.Counter {
		resp = &t_api.Response{
			Kind: t_api.ClaimTask,
			ClaimTask: &t_api.ClaimTaskResponse{
				Status: t_api.StatusTaskWrongCounter,
			},
		}
	}

	// Can't claim a task that is already timedout. Must be enqued again by enqueueTask coroutine.
	if task.ClaimTimeout < c.Time() {
		resp = &t_api.Response{
			Kind: t_api.ClaimTask,
			ClaimTask: &t_api.ClaimTaskResponse{
				Status: t_api.StatusTaskAlreadyTimedOut,
			},
		}
	}

	// Can't claim a task that is already globally timedout. No work can be done on it. todo: retries...?
	if task.PromiseTimeout < c.Time() {
		resp = &t_api.Response{
			Kind: t_api.ClaimTask,
			ClaimTask: &t_api.ClaimTaskResponse{
				Status: t_api.StatusTaskAlreadyTimedOut,
			},
		}
	}

	return resp
}

func tryClaimTask(c *Coroutine, req *t_api.Request, task *task.Task) (*t_aio.Completion, error) {
	timeout := c.Time() + (req.ClaimTask.ExpiryInSeconds * 1000) // from s to ms

	completion, err := c.Yield(&t_aio.Submission{
		Kind: t_aio.Store,
		Store: &t_aio.StoreSubmission{
			Transaction: &t_aio.Transaction{
				Commands: []*t_aio.Command{
					{
						Kind: t_aio.AcquireLock,
						AcquireLock: &t_aio.AcquireLockCommand{
							ResourceId:      task.PromiseId,
							ProcessId:       req.ClaimTask.ProcessId,
							ExecutionId:     req.ClaimTask.ExecutionId,
							ExpiryInSeconds: req.ClaimTask.ExpiryInSeconds,
							Timeout:         timeout, // from s to ms
						},
					},
					// optimizing for the happy path. JUST GO FOR IT and avoid two writes.
					{
						Kind: t_aio.UpdateTask,
						UpdateTask: &t_aio.UpdateTaskCommand{
							Id:              task.Id,
							Counter:         task.Counter,
							ClaimTimeout:    task.ClaimTimeout, // todo: reset to 0 ? -- this is arbitrary...
							CompleteTimeout: timeout,           // this is a duplication of the lock timeout.
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

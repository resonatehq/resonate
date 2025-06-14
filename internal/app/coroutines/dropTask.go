package coroutines

import (
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/task"
)

func DropTask(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	req := r.Payload.(*t_api.DropTaskRequest)
	completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
		Kind: t_aio.Store,
		Tags: r.Metadata,
		Store: &t_aio.StoreSubmission{
			Transaction: &t_aio.Transaction{
				Commands: []t_aio.Command{
					&t_aio.ReadTaskCommand{
						Id: req.Id,
					},
				},
			},
		},
	})
	if err != nil {
		slog.Error("failed to read task", "req", r, "err", err)
		return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
	}

	readResult := t_aio.AsQueryTasks(completion.Store.Results[0])
	util.Assert(readResult.RowsReturned == 0 || readResult.RowsReturned == 1, "result must return 0 or 1 rows")

	if readResult.RowsReturned == 0 {
		return &t_api.Response{
			Status:   t_api.StatusTaskNotFound,
			Metadata: r.Metadata,
			Payload:  &t_api.DropTaskResponse{},
		}, nil
	}

	t, err := readResult.Records[0].Task()
	if err != nil {
		slog.Error("failed to parse task", "req", r, "err", err)
		return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
	}

	// Only try to drop task that are Claimed, otherwise the intended
	// effect of droping a task is already done.
	if t.State != task.Claimed {
		return &t_api.Response{
			Status:   t_api.StatusOK,
			Metadata: r.Metadata,
			Payload:  &t_api.DropTaskResponse{},
		}, nil
	}

	if t.Counter != req.Counter {
		return &t_api.Response{
			Status:   t_api.StatusTaskInvalidCounter,
			Metadata: r.Metadata,
			Payload:  &t_api.DropTaskResponse{},
		}, nil
	}

	// When updating the task we don't want to reset the counter
	// instead increase the counter by one in case the client tries
	// to claim the same old task again. Resetting the counter
	// to 1 will cause that nodes with stale tasks could claim them.
	completion, err = gocoro.YieldAndAwait(c, &t_aio.Submission{
		Kind: t_aio.Store,
		Tags: r.Metadata,
		Store: &t_aio.StoreSubmission{
			Transaction: &t_aio.Transaction{
				Commands: []t_aio.Command{
					&t_aio.UpdateTaskCommand{
						Id:             req.Id,
						ProcessId:      nil,
						State:          task.Init,
						Counter:        req.Counter + 1,
						Attempt:        0,
						Ttl:            0,
						ExpiresAt:      0,
						CurrentStates:  []task.State{task.Claimed},
						CurrentCounter: req.Counter,
					},
				},
			},
		},
	})

	if err != nil {
		slog.Error("failed to drop task", "req", r, "err", err)
		return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
	}

	util.Assert(completion.Store != nil, "completion must not be nil")
	updateResult := t_aio.AsAlterTasks(completion.Store.Results[0])
	util.Assert(updateResult.RowsAffected == 0 || updateResult.RowsAffected == 1, "result must return 0 or 1 rows")

	if updateResult.RowsAffected == 1 {
		return &t_api.Response{
			Status:   t_api.StatusCreated,
			Metadata: r.Metadata,
			Payload:  &t_api.DropTaskResponse{},
		}, nil
	} else {
		// It's possible that the task was modified by another coroutine
		// while we were trying to update it. In that case, we should just retry.
		return DropTask(c, r)
	}
}

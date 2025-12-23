package coroutines

import (
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/task"
)

func DropTask(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	req := r.Payload.(*t_api.DropTaskRequest)
	metrics := c.Get("metrics").(*metrics.Metrics)

	var status t_api.StatusCode
	var t *task.Task

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

	util.Assert(completion.Store != nil, "completion must not be nil")
	result := t_aio.AsQueryTasks(completion.Store.Results[0])
	util.Assert(result.RowsReturned == 0 || result.RowsReturned == 1, "result must return 0 or 1 rows")

	if result.RowsReturned == 1 {
		t, err = result.Records[0].Task()
		if err != nil {
			slog.Error("failed to parse task", "req", r, "err", err)
			return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
		}

		if t.State.In(task.Init | task.Enqueued | task.Completed | task.Timedout) {
			status = t_api.StatusOK
		} else if t.Counter != req.Counter {
			status = t_api.StatusTaskInvalidCounter
		} else {
			// When updating the task we don't want to reset the counter
			// instead increase the counter by one in case the client tries
			// to claim the same old task again. Resetting the counter
			// to 1 would mean that nodes with stale tasks could claim them.
			completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
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
			result := t_aio.AsAlterTasks(completion.Store.Results[0])
			util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

			if result.RowsAffected == 1 {
				// set status
				status = t_api.StatusCreated

				// update task
				t.ProcessId = nil
				t.State = task.Init
				t.Counter = req.Counter + 1
				t.Attempt = 0
				t.Ttl = 0
				t.ExpiresAt = 0

				// count tasks
				metrics.TasksTotal.WithLabelValues("dropped").Inc()
			} else {
				// It's possible that the task was modified by another coroutine
				// while we were trying to complete. In that case, we should just retry.
				return DropTask(c, r)
			}
		}
	} else {
		status = t_api.StatusTaskNotFound
	}

	util.Assert(status != 0, "status must be set")
	util.Assert(status != t_api.StatusCreated || t != nil, "task must be non nil if status created")

	return &t_api.Response{
		Status:   status,
		Metadata: r.Metadata,
		Payload: &t_api.DropTaskResponse{
			Task: t,
		},
	}, nil
}

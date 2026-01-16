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

func CompleteTask(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	req := r.Data.(*t_api.TaskCompleteRequest)
	metrics := c.Get("metrics").(*metrics.Metrics)

	var status t_api.StatusCode
	var t *task.Task

	completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
		Kind: t_aio.Store,
		Tags: r.Head,
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

		if t.State == task.Completed || t.State == task.Timedout {
			status = t_api.StatusOK
		} else if t.State == task.Init || t.State == task.Enqueued {
			status = t_api.StatusTaskInvalidState
		} else if t.Counter != req.Counter {
			status = t_api.StatusTaskInvalidCounter
		} else {
			completedOn := c.Time()
			completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
				Kind: t_aio.Store,
				Tags: r.Head,
				Store: &t_aio.StoreSubmission{
					Transaction: &t_aio.Transaction{
						Commands: []t_aio.Command{
							&t_aio.UpdateTaskCommand{
								Id:             req.Id,
								ProcessId:      nil,
								State:          task.Completed,
								Counter:        req.Counter,
								Attempt:        0,
								Ttl:            0,
								ExpiresAt:      0,
								CompletedOn:    &completedOn,
								CurrentStates:  []task.State{task.Claimed},
								CurrentCounter: req.Counter,
							},
						},
					},
				},
			})
			if err != nil {
				slog.Error("failed to complete task", "req", r, "err", err)
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
				t.State = task.Completed
				t.Attempt = 0
				t.Ttl = 0
				t.ExpiresAt = 0
				t.CompletedOn = &completedOn

				// count tasks
				metrics.TasksTotal.WithLabelValues("completed").Inc()
			} else {
				// It's possible that the task was modified by another coroutine
				// while we were trying to complete. In that case, we should just retry.
				return CompleteTask(c, r)
			}
		}
	} else {
		status = t_api.StatusTaskNotFound
	}

	util.Assert(status != 0, "status must be set")
	util.Assert(status != t_api.StatusCreated || t != nil, "task must be non nil if status created")

	return &t_api.Response{
		Status:   status,
		Head: r.Head,
		Data: &t_api.TaskCompleteResponse{
			Task: t,
		},
	}, nil
}

package coroutines

import (
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/task"
)

func HeartbeatTask(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
		Kind: t_aio.Store,
		Tags: r.Tags,
		Store: &t_aio.StoreSubmission{
			Transaction: &t_aio.Transaction{
				Commands: []*t_aio.Command{
					{
						Kind: t_aio.HeartbeatTask,
						HeartbeatTask: &t_aio.HeartbeatTaskCommand{
							Id:      r.HeartbeatTask.Id,
							Counter: r.HeartbeatTask.Counter,
							Time:    c.Time(),
						},
					},
				},
			},
		},
	})
	if err != nil {
		slog.Error("failed to heartbeat task", "req", r, "err", err)
		return nil, t_api.NewResonateError(t_api.ErrAIOStoreFailure, "failed to heartbeat task", err)
	}

	util.Assert(completion.Store != nil, "completion must not be nil")
	result := completion.Store.Results[0].UpdateTask
	util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

	var res *t_api.Response

	if result.RowsAffected == 1 {
		res = &t_api.Response{
			Kind: t_api.HeartbeatTask,
			Tags: r.Tags,
			HeartbeatTask: &t_api.HeartbeatTaskResponse{
				Status: t_api.StatusCreated,
			},
		}
	} else {
		completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
			Kind: t_aio.Store,
			Tags: r.Tags,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []*t_aio.Command{
						{
							Kind: t_aio.ReadTask,
							ReadTask: &t_aio.ReadTaskCommand{
								Id: r.HeartbeatTask.Id,
							},
						},
					},
				},
			},
		})
		if err != nil {
			slog.Error("failed to read task", "req", r, "err", err)
			return nil, t_api.NewResonateError(t_api.ErrAIOStoreFailure, "failed to read task", err)
		}

		util.Assert(completion.Store != nil, "completion must not be nil")
		result := completion.Store.Results[0].ReadTask
		util.Assert(result.RowsReturned == 0 || result.RowsReturned == 1, "result must return 0 or 1 rows")

		if result.RowsReturned == 1 {
			t := result.Records[0]

			if t.State == task.Claimed && t.Counter == r.HeartbeatTask.Counter {
				// It's possible that the task was modified by another coroutine
				// while we were trying to heartbeat. In that case, we should just retry.
				return HeartbeatTask(c, r)
			}

			var status t_api.ResponseStatus

			if t.State == task.Completed || t.State == task.Timedout {
				status = t_api.StatusTaskAlreadyCompleted
			} else if t.State == task.Init || t.State == task.Enqueued {
				status = t_api.StatusTaskInvalidState
			} else {
				util.Assert(t.Counter != r.HeartbeatTask.Counter, "counters must not match")
				status = t_api.StatusTaskInvalidCounter
			}

			res = &t_api.Response{
				Kind: t_api.HeartbeatTask,
				Tags: r.Tags,
				HeartbeatTask: &t_api.HeartbeatTaskResponse{
					Status: status,
				},
			}
		} else {
			res = &t_api.Response{
				Kind: t_api.HeartbeatTask,
				Tags: r.Tags,
				HeartbeatTask: &t_api.HeartbeatTaskResponse{
					Status: t_api.StatusTaskNotFound,
				},
			}
		}
	}

	util.Assert(res != nil, "response must not be nil")
	return res, nil
}

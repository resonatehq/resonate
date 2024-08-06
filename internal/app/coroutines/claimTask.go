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
	completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
		Kind: t_aio.Store,
		Tags: r.Tags,
		Store: &t_aio.StoreSubmission{
			Transaction: &t_aio.Transaction{
				Commands: []*t_aio.Command{
					{
						Kind: t_aio.UpdateTask,
						UpdateTask: &t_aio.UpdateTaskCommand{
							Id:             r.ClaimTask.Id,
							State:          task.Claimed,
							Counter:        r.ClaimTask.Counter,
							Frequency:      r.ClaimTask.Frequency,
							Expiration:     c.Time() + int64(r.ClaimTask.Frequency), // time to expire unless heartbeated
							CurrentStates:  []task.State{task.Init, task.Enqueued},
							CurrentCounter: r.ClaimTask.Counter,
						},
					},
				},
			},
		},
	})
	if err != nil {
		slog.Error("failed to claim task", "req", r, "err", err)
		return nil, t_api.NewResonateError(t_api.ErrAIOStoreFailure, "failed to claim task", err)
	}

	util.Assert(completion.Store != nil, "completion must not be nil")
	result := completion.Store.Results[0].UpdateTask
	util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

	var res *t_api.Response

	if result.RowsAffected == 1 {
		res = &t_api.Response{
			Kind: t_api.ClaimTask,
			Tags: r.Tags,
			ClaimTask: &t_api.ClaimTaskResponse{
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
								Id: r.ClaimTask.Id,
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

			if t.State.In(task.Init|task.Enqueued) && t.Counter == r.ClaimTask.Counter {
				// It's possible that the task was modified by another coroutine
				// while we were trying to claim. In that case, we should just retry.
				return ClaimTask(c, r)
			}

			var status t_api.ResponseStatus

			if t.State == task.Claimed {
				status = t_api.StatusTaskAlreadyClaimed
			} else if t.State == task.Completed || t.State == task.Timedout {
				status = t_api.StatusTaskAlreadyCompleted
			} else {
				util.Assert(t.Counter != r.ClaimTask.Counter, "counters must not match")
				status = t_api.StatusTaskInvalidCounter
			}

			res = &t_api.Response{
				Kind: t_api.ClaimTask,
				Tags: r.Tags,
				ClaimTask: &t_api.ClaimTaskResponse{
					Status: status,
				},
			}
		} else {
			res = &t_api.Response{
				Kind: t_api.ClaimTask,
				Tags: r.Tags,
				ClaimTask: &t_api.ClaimTaskResponse{
					Status: t_api.StatusTaskNotFound,
				},
			}
		}
	}

	util.Assert(res != nil, "response must not be nil")
	return res, nil
}

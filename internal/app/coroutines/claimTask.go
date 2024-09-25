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

func ClaimTask(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	util.Assert(r.ClaimTask.ProcessId != "", "process id must be set")
	util.Assert(r.ClaimTask.Frequency > 0, "frequency must be greater than 0")

	var status t_api.StatusCode
	var t *task.Task

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
		return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
	}

	util.Assert(completion.Store != nil, "completion must not be nil")
	result := completion.Store.Results[0].ReadTask
	util.Assert(result.RowsReturned == 0 || result.RowsReturned == 1, "result must return 0 or 1 rows")

	if result.RowsReturned == 1 {
		t, err = result.Records[0].Task()
		if err != nil {
			slog.Error("failed to parse task", "req", r, "err", err)
			return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
		}

		if t.State == task.Claimed {
			status = t_api.StatusTaskAlreadyClaimed
		} else if t.State == task.Completed || t.State == task.Timedout {
			status = t_api.StatusTaskAlreadyCompleted
		} else if t.Counter != r.ClaimTask.Counter {
			status = t_api.StatusTaskInvalidCounter
		} else {
			expiration := c.Time() + int64(r.ClaimTask.Frequency)
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
									ProcessId:      &r.ClaimTask.ProcessId,
									State:          task.Claimed,
									Counter:        r.ClaimTask.Counter,
									Attempt:        t.Attempt,
									Frequency:      r.ClaimTask.Frequency,
									Expiration:     expiration, // time to expire unless heartbeated
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
				return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
			}

			util.Assert(completion.Store != nil, "completion must not be nil")
			result := completion.Store.Results[0].UpdateTask
			util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

			if result.RowsAffected == 1 {
				commands := []*t_aio.Command{{
					Kind:        t_aio.ReadPromise,
					ReadPromise: &t_aio.ReadPromiseCommand{Id: t.Mesg.Root},
				}}

				if t.Mesg.Type == message.Resume {
					commands = append(commands, &t_aio.Command{
						Kind:        t_aio.ReadPromise,
						ReadPromise: &t_aio.ReadPromiseCommand{Id: t.Mesg.Leaf},
					})
				}

				completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
					Kind: t_aio.Store,
					Tags: r.Tags,
					Store: &t_aio.StoreSubmission{
						Transaction: &t_aio.Transaction{
							Commands: commands,
						},
					},
				})

				if err != nil {
					slog.Error("failed to read promises", "req", r, "err", err)
					return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
				}

				util.Assert(completion.Store != nil, "completion must not be nil")
				util.Assert(len(completion.Store.Results) == len(commands), "number of results must match number of commands")
				util.Assert(completion.Store.Results[0].ReadPromise != nil, "result must not be nil")
				util.Assert(t.Mesg.Type != message.Resume || completion.Store.Results[1].ReadPromise != nil, "if resume, result must not be nil")

				var root, leaf *promise.Promise

				if completion.Store.Results[0].ReadPromise.RowsReturned == 1 {
					root, err = completion.Store.Results[0].ReadPromise.Records[0].Promise()
					if err != nil {
						slog.Error("failed to parse promises", "err", err)
					}
				}

				if t.Mesg.Type == message.Resume && completion.Store.Results[1].ReadPromise.RowsReturned == 1 {
					leaf, err = completion.Store.Results[1].ReadPromise.Records[0].Promise()
					if err != nil {
						slog.Error("failed to parse promises", "err", err)
					}
				}

				// set status
				status = t_api.StatusCreated

				// update task
				t.ProcessId = &r.ClaimTask.ProcessId
				t.State = task.Claimed
				t.Frequency = r.ClaimTask.Frequency
				t.Expiration = expiration
				t.Mesg.SetPromises(root, leaf)
			} else {
				// It's possible that the task was modified by another coroutine
				// while we were trying to claim. In that case, we should just retry.
				return ClaimTask(c, r)
			}
		}
	} else {
		status = t_api.StatusTaskNotFound
	}

	util.Assert(status != 0, "status must be set")
	util.Assert(status != t_api.StatusCreated || t != nil, "task must be non nil if status created")

	return &t_api.Response{
		Kind: t_api.ClaimTask,
		Tags: r.Tags,
		ClaimTask: &t_api.ClaimTaskResponse{
			Status: status,
			Task:   t,
		},
	}, nil
}

package coroutines

import (
	"fmt"
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/message"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/task"
)

func ClaimTask(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	req := r.Payload.(*t_api.ClaimTaskRequest)
	util.Assert(req.ProcessId != "", "process id must be set")
	util.Assert(req.Ttl >= 0, "ttl must be greater than or equal to 0")

	config := c.Get("config").(*system.Config)

	var status t_api.StatusCode
	var t *task.Task
	var rp, lp *promise.Promise
	var rh, lh string

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

		if t.State == task.Claimed {
			status = t_api.StatusTaskAlreadyClaimed
		} else if t.State == task.Completed || t.State == task.Timedout {
			status = t_api.StatusTaskAlreadyCompleted
		} else if t.Counter != req.Counter {
			status = t_api.StatusTaskInvalidCounter
		} else {
			// guard against overflow
			expiresAt := util.ClampAddInt64(c.Time(), req.Ttl)

			completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
				Kind: t_aio.Store,
				Tags: r.Metadata,
				Store: &t_aio.StoreSubmission{
					Transaction: &t_aio.Transaction{
						Commands: []t_aio.Command{
							&t_aio.UpdateTaskCommand{
								Id:             req.Id,
								ProcessId:      &req.ProcessId,
								State:          task.Claimed,
								Counter:        req.Counter,
								Attempt:        t.Attempt,
								Ttl:            req.Ttl,
								ExpiresAt:      expiresAt, // time to expire unless heartbeated
								CurrentStates:  []task.State{task.Init, task.Enqueued},
								CurrentCounter: req.Counter,
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
			result := t_aio.AsAlterTasks(completion.Store.Results[0])
			util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

			if result.RowsAffected == 1 {
				commands := []t_aio.Command{
					&t_aio.ReadPromiseCommand{Id: t.Mesg.Root},
				}

				if t.Mesg.Type == message.Resume {
					commands = append(commands, &t_aio.ReadPromiseCommand{Id: t.Mesg.Leaf})
				}

				completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
					Kind: t_aio.Store,
					Tags: r.Metadata,
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

				rootPromiseRes := t_aio.AsQueryPromises(completion.Store.Results[0])
				var leafPromiseRes *t_aio.QueryPromisesResult
				if t.Mesg.Type == message.Resume {
					leafPromiseRes = t_aio.AsQueryPromises(completion.Store.Results[1])
				}

				// set promises
				rh = fmt.Sprintf("%s/promises/%s", config.Url, t.Mesg.Root)

				if t.Mesg.Type == message.Resume {
					lh = fmt.Sprintf("%s/promises/%s", config.Url, t.Mesg.Leaf)
				}

				if rootPromiseRes.RowsReturned == 1 {
					rp, err = rootPromiseRes.Records[0].Promise()
					if err != nil {
						slog.Error("failed to parse promise", "err", err)
					}
				}

				if t.Mesg.Type == message.Resume && leafPromiseRes.RowsReturned == 1 {
					lp, err = leafPromiseRes.Records[0].Promise()
					if err != nil {
						slog.Error("failed to parse promise", "err", err)
					}
				}

				// set status
				status = t_api.StatusCreated

				// update task
				t.ProcessId = &req.ProcessId
				t.State = task.Claimed
				t.Ttl = req.Ttl
				t.ExpiresAt = expiresAt
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
		Status:   status,
		Metadata: r.Metadata,
		Payload: &t_api.ClaimTaskResponse{
			Task:            t,
			RootPromise:     rp,
			LeafPromise:     lp,
			RootPromiseHref: rh,
			LeafPromiseHref: lh,
		},
	}, nil
}

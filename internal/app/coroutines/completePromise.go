package coroutines

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/task"
)

func CompletePromise(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	req := r.Payload.(*t_api.CompletePromiseRequest)
	completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
		Kind: t_aio.Store,
		Tags: r.Metadata,
		Store: &t_aio.StoreSubmission{
			Transaction: &t_aio.Transaction{
				Fence: r.Fence,
				Commands: []t_aio.Command{
					&t_aio.ReadPromiseCommand{
						Id: req.Id,
					},
				},
			},
		},
	})

	if err != nil {
		slog.Error("failed to read promise", "req", r, "err", err)
		return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
	}

	if !completion.Store.Valid {
		return nil, t_api.NewError(t_api.StatusTaskPreconditionFailed, errors.New("the specified task is not valid"))
	}

	util.Assert(completion.Store != nil, "completion must not be nil")

	result := t_aio.AsQueryPromises(completion.Store.Results[0])
	util.Assert(result.RowsReturned == 0 || result.RowsReturned == 1, "result must return 0 or 1 rows")

	var res *t_api.Response

	if result.RowsReturned == 1 {
		p, err := result.Records[0].Promise()
		if err != nil {
			slog.Error("failed to parse promise record", "record", result.Records[0], "err", err)
			return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
		}

		if p.State == promise.Pending {
			var cmd *t_aio.UpdatePromiseCommand
			var status t_api.StatusCode

			if c.Time() < p.Timeout {
				cmd = &t_aio.UpdatePromiseCommand{
					Id:             req.Id,
					State:          req.State,
					Value:          req.Value,
					IdempotencyKey: req.IdempotencyKey,
					CompletedOn:    c.Time(),
				}
				status = t_api.StatusCreated
			} else {
				cmd = &t_aio.UpdatePromiseCommand{
					Id:             req.Id,
					State:          promise.GetTimedoutState(p),
					Value:          promise.Value{},
					IdempotencyKey: nil,
					CompletedOn:    p.Timeout,
				}

				if cmd.State == promise.Resolved {
					status = t_api.StatusPromiseAlreadyResolved
				} else if req.Strict {
					status = t_api.StatusPromiseAlreadyTimedout
				} else {
					status = t_api.StatusOK
				}
			}

			ok, err := gocoro.SpawnAndAwait(c, completePromise(r.Metadata, r.Fence, cmd))
			if err != nil {
				return nil, err
			}

			if !ok {
				// It's possible that the promise was completed by another coroutine
				// while we were completing. In that case, we should just retry.
				return CompletePromise(c, r)
			}

			res = &t_api.Response{
				Status:   status,
				Metadata: r.Metadata,
				Payload: &t_api.CompletePromiseResponse{
					Promise: &promise.Promise{
						Id:                        p.Id,
						State:                     cmd.State,
						Param:                     p.Param,
						Value:                     cmd.Value,
						Timeout:                   p.Timeout,
						IdempotencyKeyForCreate:   p.IdempotencyKeyForCreate,
						IdempotencyKeyForComplete: cmd.IdempotencyKey,
						Tags:                      p.Tags,
						CreatedOn:                 p.CreatedOn,
						CompletedOn:               &cmd.CompletedOn,
					},
				},
			}
		} else {
			status := alreadyCompletedStatus(p.State)
			strict := req.Strict && p.State != req.State
			timeout := !req.Strict && p.State == promise.Timedout

			if (!strict && p.IdempotencyKeyForComplete.Match(req.IdempotencyKey)) || timeout {
				status = t_api.StatusOK
			}

			res = &t_api.Response{
				Status:   status,
				Metadata: r.Metadata,
				Payload: &t_api.CompletePromiseResponse{
					Promise: p,
				},
			}
		}
	} else {
		res = &t_api.Response{
			Status:   t_api.StatusPromiseNotFound,
			Metadata: r.Metadata,
			Payload:  &t_api.CompletePromiseResponse{},
		}
	}

	util.Assert(res != nil, "response must not be nil")
	return res, nil
}

func completePromise(tags map[string]string, fence *task.FencingToken, updatePromiseCmd *t_aio.UpdatePromiseCommand, additionalCmds ...t_aio.Command) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, bool] {
	if updatePromiseCmd.Value.Headers == nil {
		updatePromiseCmd.Value.Headers = map[string]string{}
	}
	if updatePromiseCmd.Value.Data == nil {
		updatePromiseCmd.Value.Data = []byte{}
	}

	return func(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, bool]) (bool, error) {
		commands := []t_aio.Command{
			updatePromiseCmd,
			&t_aio.CompleteTasksCommand{
				RootPromiseId: updatePromiseCmd.Id,
				CompletedOn:   c.Time(),
			},
			&t_aio.CreateTasksCommand{
				PromiseId: updatePromiseCmd.Id,
				CreatedOn: c.Time(),
			},
			&t_aio.DeleteCallbacksCommand{
				PromiseId: updatePromiseCmd.Id,
			},
		}

		// add additional commands
		commands = append(commands, additionalCmds...)

		completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
			Kind: t_aio.Store,
			Tags: tags,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Fence:    fence,
					Commands: commands,
				},
			},
		})

		if err != nil {
			slog.Error("failed to update promise", "err", err)
			return false, t_api.NewError(t_api.StatusAIOStoreError, err)
		}

		if !completion.Store.Valid {
			return false, t_api.NewError(t_api.StatusTaskPreconditionFailed, errors.New("the specified task is not valid"))
		}

		util.Assert(completion.Store != nil, "completion must not be nil")
		util.Assert(len(completion.Store.Results) == len(commands), "completion must have same number of results as commands")

		updatePromiseResult := t_aio.AsAlterPromises(completion.Store.Results[0])
		_ = t_aio.AsAlterTasks(completion.Store.Results[1]) // serves as type assertion
		createTasksResult := t_aio.AsAlterTasks(completion.Store.Results[2])
		deleteCallbacksRusult := t_aio.AsAlterCallbacks(completion.Store.Results[3])

		util.Assert(updatePromiseResult != nil, "result must not be nil")
		util.Assert(updatePromiseResult.RowsAffected == 0 || updatePromiseResult.RowsAffected == 1, "result must return 0 or 1 rows")
		util.Assert(createTasksResult.RowsAffected == deleteCallbacksRusult.RowsAffected, "created rows must equal deleted rows")

		return updatePromiseResult.RowsAffected == 1, nil
	}
}

// Helper functions

func alreadyCompletedStatus(state promise.State) t_api.StatusCode {
	switch state {
	case promise.Resolved:
		return t_api.StatusPromiseAlreadyResolved
	case promise.Rejected:
		return t_api.StatusPromiseAlreadyRejected
	case promise.Canceled:
		return t_api.StatusPromiseAlreadyCanceled
	case promise.Timedout:
		return t_api.StatusPromiseAlreadyTimedout
	default:
		panic(fmt.Sprintf("invalid promise state: %s", state))
	}
}

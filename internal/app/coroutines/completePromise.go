package coroutines

import (
	"fmt"
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
)

func CompletePromise(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	completePromiseReq := r.Payload.(*t_api.CompletePromiseRequest)
	completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
		Kind: t_aio.Store,
		Tags: r.Metadata,
		Store: &t_aio.StoreSubmission{
			Transaction: &t_aio.Transaction{
				Commands: []*t_aio.Command{
					{
						Kind: t_aio.ReadPromise,
						ReadPromise: &t_aio.ReadPromiseCommand{
							Id: completePromiseReq.Id,
						},
					},
				},
			},
		},
	})

	if err != nil {
		slog.Error("failed to read promise", "req", r, "err", err)
		return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
	}

	util.Assert(completion.Store != nil, "completion must not be nil")

	result := completion.Store.Results[0].ReadPromise
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
					Id:             completePromiseReq.Id,
					State:          completePromiseReq.State,
					Value:          completePromiseReq.Value,
					IdempotencyKey: completePromiseReq.IdempotencyKey,
					CompletedOn:    c.Time(),
				}
				status = t_api.StatusCreated
			} else {
				cmd = &t_aio.UpdatePromiseCommand{
					Id:             completePromiseReq.Id,
					State:          promise.GetTimedoutState(p),
					Value:          promise.Value{},
					IdempotencyKey: nil,
					CompletedOn:    p.Timeout,
				}

				if cmd.State == promise.Resolved {
					status = t_api.StatusPromiseAlreadyResolved
				} else if completePromiseReq.Strict {
					status = t_api.StatusPromiseAlreadyTimedout
				} else {
					status = t_api.StatusOK
				}
			}

			ok, err := gocoro.SpawnAndAwait(c, completePromise(r.Metadata, cmd))
			if err != nil {
				return nil, err
			}

			if !ok {
				// It's possible that the promise was completed by another coroutine
				// while we were completing. In that case, we should just retry.
				return CompletePromise(c, r)
			}

			res = &t_api.Response{
				Kind: t_api.CompletePromise,
				Tags: r.Metadata,
				CompletePromise: &t_api.CompletePromiseResponse{
					Status: status,
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
			strict := completePromiseReq.Strict && p.State != completePromiseReq.State
			timeout := !completePromiseReq.Strict && p.State == promise.Timedout

			if (!strict && p.IdempotencyKeyForComplete.Match(completePromiseReq.IdempotencyKey)) || timeout {
				status = t_api.StatusOK
			}

			res = &t_api.Response{
				Kind: t_api.CompletePromise,
				Tags: r.Metadata,
				CompletePromise: &t_api.CompletePromiseResponse{
					Status:  status,
					Promise: p,
				},
			}
		}
	} else {
		res = &t_api.Response{
			Kind: t_api.CompletePromise,
			Tags: r.Metadata,
			CompletePromise: &t_api.CompletePromiseResponse{
				Status: t_api.StatusPromiseNotFound,
			},
		}
	}

	util.Assert(res != nil, "response must not be nil")
	return res, nil
}

func completePromise(tags map[string]string, cmd *t_aio.UpdatePromiseCommand, additionalCmds ...*t_aio.Command) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, bool] {
	if cmd.Value.Headers == nil {
		cmd.Value.Headers = map[string]string{}
	}
	if cmd.Value.Data == nil {
		cmd.Value.Data = []byte{}
	}

	return func(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, bool]) (bool, error) {
		commands := []*t_aio.Command{
			{
				Kind:          t_aio.UpdatePromise,
				UpdatePromise: cmd,
			},
			{
				Kind: t_aio.CompleteTasks,
				CompleteTasks: &t_aio.CompleteTasksCommand{
					RootPromiseId: cmd.Id,
					CompletedOn:   c.Time(),
				},
			},
			{
				Kind: t_aio.CreateTasks,
				CreateTasks: &t_aio.CreateTasksCommand{
					PromiseId: cmd.Id,
					CreatedOn: c.Time(),
				},
			},
			{
				Kind: t_aio.DeleteCallbacks,
				DeleteCallbacks: &t_aio.DeleteCallbacksCommand{
					PromiseId: cmd.Id,
				},
			},
		}

		// add additional commands
		commands = append(commands, additionalCmds...)

		completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
			Kind: t_aio.Store,
			Tags: tags,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: commands,
				},
			},
		})

		if err != nil {
			slog.Error("failed to update promise", "err", err)
			return false, t_api.NewError(t_api.StatusAIOStoreError, err)
		}

		util.Assert(completion.Store != nil, "completion must not be nil")
		util.Assert(len(completion.Store.Results) == len(commands), "completion must have same number of results as commands")
		util.Assert(completion.Store.Results[0].UpdatePromise != nil, "result must not be nil")
		util.Assert(completion.Store.Results[0].UpdatePromise.RowsAffected == 0 || completion.Store.Results[0].UpdatePromise.RowsAffected == 1, "result must return 0 or 1 rows")
		util.Assert(completion.Store.Results[1].CompleteTasks != nil, "result must not be nil")
		util.Assert(completion.Store.Results[2].CreateTasks != nil, "result must not be nil")
		util.Assert(completion.Store.Results[3].DeleteCallbacks != nil, "result must not be nil")
		util.Assert(completion.Store.Results[2].CreateTasks.RowsAffected == completion.Store.Results[3].DeleteCallbacks.RowsAffected, "created rows must equal deleted rows")

		return completion.Store.Results[0].UpdatePromise.RowsAffected == 1, nil
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

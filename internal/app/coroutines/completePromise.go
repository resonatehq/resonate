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
	completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
		Kind: t_aio.Store,
		Tags: r.Tags,
		Store: &t_aio.StoreSubmission{
			Transaction: &t_aio.Transaction{
				Commands: []*t_aio.Command{
					{
						Kind: t_aio.ReadPromise,
						ReadPromise: &t_aio.ReadPromiseCommand{
							Id: r.CompletePromise.Id,
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
			var con int64
			var req *t_api.Request
			var status t_api.StatusCode

			if c.Time() < p.Timeout {
				con = c.Time()
				req = r
				status = t_api.StatusCreated
			} else {
				state := promise.GetTimedoutState(p)

				con = p.Timeout
				req = &t_api.Request{
					Kind: t_api.CompletePromise,
					Tags: r.Tags,
					CompletePromise: &t_api.CompletePromiseRequest{
						Id:    r.CompletePromise.Id,
						State: state,
					},
				}

				if state == promise.Resolved {
					status = t_api.StatusPromiseAlreadyResolved
				} else if r.CompletePromise.Strict {
					status = t_api.StatusPromiseAlreadyTimedout
				} else {
					status = t_api.StatusOK
				}
			}

			ok, err := gocoro.SpawnAndAwait(c, completePromise(con, req))
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
				Tags: req.Tags,
				CompletePromise: &t_api.CompletePromiseResponse{
					Status: status,
					Promise: &promise.Promise{
						Id:                        p.Id,
						State:                     req.CompletePromise.State,
						Param:                     p.Param,
						Value:                     req.CompletePromise.Value,
						Timeout:                   p.Timeout,
						IdempotencyKeyForCreate:   p.IdempotencyKeyForCreate,
						IdempotencyKeyForComplete: req.CompletePromise.IdempotencyKey,
						Tags:                      p.Tags,
						CreatedOn:                 p.CreatedOn,
						CompletedOn:               &con,
					},
				},
			}
		} else {
			status := alreadyCompletedStatus(p.State)
			strict := r.CompletePromise.Strict && p.State != r.CompletePromise.State
			timeout := !r.CompletePromise.Strict && p.State == promise.Timedout

			if (!strict && p.IdempotencyKeyForComplete.Match(r.CompletePromise.IdempotencyKey)) || timeout {
				status = t_api.StatusOK
			}

			res = &t_api.Response{
				Kind: t_api.CompletePromise,
				Tags: r.Tags,
				CompletePromise: &t_api.CompletePromiseResponse{
					Status:  status,
					Promise: p,
				},
			}
		}
	} else {
		res = &t_api.Response{
			Kind: t_api.CompletePromise,
			Tags: r.Tags,
			CompletePromise: &t_api.CompletePromiseResponse{
				Status: t_api.StatusPromiseNotFound,
			},
		}
	}

	util.Assert(res != nil, "response must not be nil")
	return res, nil
}

func completePromise(completedOn int64, r *t_api.Request) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, bool] {
	util.Assert(r.CompletePromise != nil, "complete promise req must not be nil")

	if r.CompletePromise.Value.Headers == nil {
		r.CompletePromise.Value.Headers = map[string]string{}
	}
	if r.CompletePromise.Value.Data == nil {
		r.CompletePromise.Value.Data = []byte{}
	}

	return func(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, bool]) (bool, error) {
		completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
			Kind: t_aio.Store,
			Tags: r.Tags,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []*t_aio.Command{
						{
							Kind: t_aio.UpdatePromise,
							UpdatePromise: &t_aio.UpdatePromiseCommand{
								Id:             r.CompletePromise.Id,
								State:          r.CompletePromise.State,
								Value:          r.CompletePromise.Value,
								IdempotencyKey: r.CompletePromise.IdempotencyKey,
								CompletedOn:    completedOn,
							},
						},
						{
							Kind: t_aio.CreateTasks,
							CreateTasks: &t_aio.CreateTasksCommand{
								PromiseId: r.CompletePromise.Id,
								CreatedOn: completedOn,
							},
						},
						{
							Kind: t_aio.DeleteCallbacks,
							DeleteCallbacks: &t_aio.DeleteCallbacksCommand{
								PromiseId: r.CompletePromise.Id,
							},
						},
					},
				},
			},
		})

		if err != nil {
			slog.Error("failed to update promise", "req", r, "err", err)
			return false, t_api.NewError(t_api.StatusAIOStoreError, err)
		}

		util.Assert(completion.Store != nil, "completion must not be nil")
		util.Assert(len(completion.Store.Results) == 3, "completion must have three results")
		util.Assert(completion.Store.Results[0].UpdatePromise != nil, "result must not be nil")
		util.Assert(completion.Store.Results[0].UpdatePromise.RowsAffected == 0 || completion.Store.Results[0].UpdatePromise.RowsAffected == 1, "result must return 0 or 1 rows")
		util.Assert(completion.Store.Results[1].CreateTasks != nil, "result must not be nil")
		util.Assert(completion.Store.Results[2].DeleteCallbacks != nil, "result must not be nil")
		util.Assert(completion.Store.Results[1].CreateTasks.RowsAffected == completion.Store.Results[2].DeleteCallbacks.RowsAffected, "created rows must equal deleted rows")

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

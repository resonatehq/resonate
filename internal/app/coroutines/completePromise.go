package coroutines

import (
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
)

func CompletePromise(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	if r.CompletePromise.Value.Headers == nil {
		r.CompletePromise.Value.Headers = map[string]string{}
	}
	if r.CompletePromise.Value.Data == nil {
		r.CompletePromise.Value.Data = []byte{}
	}

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
		return nil, t_api.NewResonateError(t_api.ErrAIOStoreFailure, "failed to read promise", err)
	}

	util.Assert(completion.Store != nil, "completion must not be nil")

	result := completion.Store.Results[0].ReadPromise
	util.Assert(result.RowsReturned == 0 || result.RowsReturned == 1, "result must return 0 or 1 rows")

	var res *t_api.Response

	if result.RowsReturned == 0 {
		res = &t_api.Response{
			Kind: r.Kind,
			Tags: r.Tags,
			CompletePromise: &t_api.CompletePromiseResponse{
				Status: t_api.StatusPromiseNotFound,
			},
		}
	} else {
		p, err := result.Records[0].Promise()
		if err != nil {
			slog.Error("failed to parse promise record", "record", result.Records[0], "err", err)
			return nil, t_api.NewResonateError(t_api.ErrAIOStoreSerializationFailure, "failed to parse promise record", err)
		}

		if p.State == promise.Pending {
			if c.Time() >= p.Timeout {
				success, err := gocoro.SpawnAndAwait(c, TimeoutPromise(p))
				if err != nil {
					return nil, err
				}

				if !success {
					// It's possible that the promise was completed by another coroutine
					// while we were timing out. In that case, we should just retry.
					return CompletePromise(c, r)
				}

				// Determine the status based on the state of the promise:
				// - a timer promise transitions to resolved
				//   - status is 403
				// - a regular promise transitions to timedout
				//   - status is 403 if strict
				//   - status is 200 if not strict
				var status t_api.ResponseStatus
				state := promise.GetTimedoutState(p)

				if state == promise.Resolved {
					status = t_api.StatusPromiseAlreadyResolved
				} else if r.CompletePromise.Strict {
					status = t_api.StatusPromiseAlreadyTimedout
				} else {
					status = t_api.StatusOK
				}

				res = &t_api.Response{
					Kind: r.Kind,
					Tags: r.Tags,
					CompletePromise: &t_api.CompletePromiseResponse{
						Status: status,
						Promise: &promise.Promise{
							Id:    p.Id,
							State: state,
							Param: p.Param,
							Value: promise.Value{
								Headers: map[string]string{},
								Data:    []byte{},
							},
							Timeout:                   p.Timeout,
							IdempotencyKeyForCreate:   p.IdempotencyKeyForCreate,
							IdempotencyKeyForComplete: p.IdempotencyKeyForComplete,
							Tags:                      p.Tags,
							CreatedOn:                 p.CreatedOn,
							CompletedOn:               &p.Timeout,
						},
					},
				}
			} else {
				completedOn := c.Time()
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
									Kind: t_aio.CreateNotifications,
									CreateNotifications: &t_aio.CreateNotificationsCommand{
										PromiseId: r.CompletePromise.Id,
										Time:      completedOn,
									},
								},
								{
									Kind: t_aio.DeleteSubscriptions,
									DeleteSubscriptions: &t_aio.DeleteSubscriptionsCommand{
										PromiseId: r.CompletePromise.Id,
									},
								},
							},
						},
					},
				})

				if err != nil {
					slog.Error("failed to update promise", "req", r, "err", err)
					return nil, t_api.NewResonateError(t_api.ErrAIOStoreFailure, "failed to update promise", err)
				}

				util.Assert(completion.Store != nil, "completion must not be nil")

				result := completion.Store.Results[0].UpdatePromise
				util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

				if result.RowsAffected == 1 {
					res = &t_api.Response{
						Kind: r.Kind,
						Tags: r.Tags,
						CompletePromise: &t_api.CompletePromiseResponse{
							Status: t_api.StatusCreated,
							Promise: &promise.Promise{
								Id:                        p.Id,
								State:                     r.CompletePromise.State,
								Param:                     p.Param,
								Value:                     r.CompletePromise.Value,
								Timeout:                   p.Timeout,
								IdempotencyKeyForCreate:   p.IdempotencyKeyForCreate,
								IdempotencyKeyForComplete: r.CompletePromise.IdempotencyKey,
								Tags:                      p.Tags,
								CreatedOn:                 p.CreatedOn,
								CompletedOn:               &completedOn,
							},
						},
					}
				} else {
					// It's possible that the promise was completed by another coroutine
					// while we were completing. In that case, we should just retry.
					return CompletePromise(c, r)
				}
			}
		} else {
			status := t_api.ForbiddenStatus(p.State)
			strict := r.CompletePromise.Strict && p.State != r.CompletePromise.State
			timeout := !r.CompletePromise.Strict && p.State == promise.Timedout

			if (!strict && p.IdempotencyKeyForComplete.Match(r.CompletePromise.IdempotencyKey)) || timeout {
				status = t_api.StatusOK
			}

			res = &t_api.Response{
				Kind: r.Kind,
				Tags: r.Tags,
				CompletePromise: &t_api.CompletePromiseResponse{
					Status:  status,
					Promise: p,
				},
			}
		}
	}

	util.Assert(res != nil, "response must not be nil")
	return res, nil
}

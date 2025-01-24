package coroutines

import (
	"fmt"
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/callback"
	"github.com/resonatehq/resonate/pkg/message"
	"github.com/resonatehq/resonate/pkg/promise"
)

func CreateSubscription(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	util.Assert(r.Kind == t_api.CreateSubscription, "Request kind must be CreateSubscription")
	var res *t_api.Response

	// read the promise to see if it exists
	completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
		Kind: t_aio.Store,
		Tags: r.Tags,
		Store: &t_aio.StoreSubmission{
			Transaction: &t_aio.Transaction{
				Commands: []*t_aio.Command{
					{
						Kind: t_aio.ReadPromise,
						ReadPromise: &t_aio.ReadPromiseCommand{
							Id: r.CreateSubscription.PromiseId,
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
	util.Assert(len(completion.Store.Results) == 1, "completion must have one result")

	result := completion.Store.Results[0].ReadPromise
	util.Assert(result != nil, "result must not be nil")
	util.Assert(result.RowsReturned == 0 || result.RowsReturned == 1, "result must return 0 or 1 rows")

	if result.RowsReturned == 1 {
		p, err := result.Records[0].Promise()
		if err != nil {
			slog.Error("failed to parse promise record", "record", result.Records[0], "err", err)
			return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
		}

		// If the subscription is already created return 200 and an empty subscription
		var cb *callback.Callback
		status := t_api.StatusOK

		if p.State == promise.Pending {
			mesg := &message.Mesg{
				Type: message.Notify,
				Root: r.CreateSubscription.PromiseId,
			}

			createdOn := c.Time()

			callbackId := fmt.Sprintf("%s.%s", r.CreateSubscription.PromiseId, r.CreateSubscription.Id)
			completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
				Kind: t_aio.Store,
				Tags: r.Tags,
				Store: &t_aio.StoreSubmission{
					Transaction: &t_aio.Transaction{
						Commands: []*t_aio.Command{
							{
								Kind: t_aio.CreateCallback,
								CreateCallback: &t_aio.CreateCallbackCommand{
									Id:        callbackId,
									PromiseId: r.CreateSubscription.PromiseId,
									Recv:      r.CreateSubscription.Recv,
									Mesg:      mesg,
									Timeout:   r.CreateSubscription.Timeout,
									CreatedOn: createdOn,
								},
							},
						},
					},
				},
			})

			if err != nil {
				slog.Error("failed to create subscription", "req", r, "err", err)
				return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
			}

			util.Assert(completion.Store != nil, "completion must not be nil")
			util.Assert(len(completion.Store.Results) == 1, "completion must have one result")

			result := completion.Store.Results[0].CreateCallback
			util.Assert(result != nil, "result must not be nil")
			util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

			if result.RowsAffected == 1 {
				status = t_api.StatusCreated
				cb = &callback.Callback{
					Id:        callbackId,
					PromiseId: r.CreateSubscription.PromiseId,
					Recv:      r.CreateSubscription.Recv,
					Mesg:      mesg,
					Timeout:   r.CreateSubscription.Timeout,
					CreatedOn: createdOn,
				}
			}
		}

		res = &t_api.Response{
			Kind: t_api.CreateSubscription,
			Tags: r.Tags,
			CreateSubscription: &t_api.CreateSubscriptionResponse{
				// Status could be StatusOk or StatusCreated if the Callback Id was already present
				Status:   status,
				Callback: cb,
				Promise:  p,
			},
		}

	} else {
		res = &t_api.Response{
			Kind: t_api.CreateSubscription,
			Tags: r.Tags,
			CreateSubscription: &t_api.CreateSubscriptionResponse{
				Status: t_api.StatusPromiseNotFound,
			},
		}
	}

	util.Assert(res != nil, "response must not be nil")
	return res, nil
}

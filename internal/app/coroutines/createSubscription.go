package coroutines

import (
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/subscription"
)

func CreateSubscription(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	// default retry policy
	if r.CreateSubscription.RetryPolicy == nil {
		r.CreateSubscription.RetryPolicy = &subscription.RetryPolicy{
			Delay:    30,
			Attempts: 3,
		}
	}

	createdOn := c.Time()
	completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
		Kind: t_aio.Store,
		Tags: r.Tags,
		Store: &t_aio.StoreSubmission{
			Transaction: &t_aio.Transaction{
				Commands: []*t_aio.Command{
					{
						Kind: t_aio.CreateSubscription,
						CreateSubscription: &t_aio.CreateSubscriptionCommand{
							Id:          r.CreateSubscription.Id,
							PromiseId:   r.CreateSubscription.PromiseId,
							Url:         r.CreateSubscription.Url,
							RetryPolicy: r.CreateSubscription.RetryPolicy,
							CreatedOn:   createdOn,
						},
					},
				},
			},
		},
	})

	if err != nil {
		slog.Error("failed to create subscription", "req", r, "err", err)
		return nil, t_api.NewResonateError(t_api.ErrAIOStoreFailure, "failed to create subscription", err)
	}

	util.Assert(completion.Store != nil, "completion must not be nil")

	result := completion.Store.Results[0].CreateSubscription
	util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

	var res *t_api.Response

	if result.RowsAffected == 1 {
		res = &t_api.Response{
			Kind: t_api.CreateSubscription,
			Tags: r.Tags,
			CreateSubscription: &t_api.CreateSubscriptionResponse{
				Status: t_api.StatusCreated,
				Subscription: &subscription.Subscription{
					Id:          r.CreateSubscription.Id,
					PromiseId:   r.CreateSubscription.PromiseId,
					Url:         r.CreateSubscription.Url,
					RetryPolicy: r.CreateSubscription.RetryPolicy,
					CreatedOn:   createdOn,
				},
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
							Kind: t_aio.ReadSubscription,
							ReadSubscription: &t_aio.ReadSubscriptionCommand{
								Id:        r.CreateSubscription.Id,
								PromiseId: r.CreateSubscription.PromiseId,
							},
						},
					},
				},
			},
		})

		if err != nil {
			slog.Error("failed to read subscription", "req", r, "err", err)
			return nil, t_api.NewResonateError(t_api.ErrAIOStoreFailure, "failed to read subscription", err)
		}

		util.Assert(completion.Store != nil, "completion must not be nil")

		result := completion.Store.Results[0].ReadSubscription
		util.Assert(result.RowsReturned == 0 || result.RowsReturned == 1, "result must return 0 or 1 rows")

		if result.RowsReturned == 1 {
			subscription, err := result.Records[0].Subscription()
			if err != nil {
				slog.Error("failed to parse subscription record", "record", result.Records[0], "err", err)
				return nil, t_api.NewResonateError(t_api.ErrAIOStoreSerializationFailure, "failed to parse subscription record", err)
			}

			res = &t_api.Response{
				Kind: t_api.CreateSubscription,
				Tags: r.Tags,
				CreateSubscription: &t_api.CreateSubscriptionResponse{
					Status:       t_api.StatusOK,
					Subscription: subscription,
				},
			}
		} else {
			// It's possible that the subscription was created by another coroutine
			// while we were creating. In that case, we should just retry.
			return CreateSubscription(c, r)
		}
	}

	util.Assert(res != nil, "response must not be nil")
	return res, nil
}

package coroutines

import (
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/subscription"
)

func ReadSubscriptions(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
		Kind: t_aio.Store,
		Tags: r.Tags,
		Store: &t_aio.StoreSubmission{
			Transaction: &t_aio.Transaction{
				Commands: []*t_aio.Command{
					{
						Kind: t_aio.ReadSubscriptions,
						ReadSubscriptions: &t_aio.ReadSubscriptionsCommand{
							PromiseId: r.ReadSubscriptions.PromiseId,
							Limit:     r.ReadSubscriptions.Limit,
							SortId:    r.ReadSubscriptions.SortId,
						},
					},
				},
			},
		},
	})

	if err != nil {
		slog.Error("failed to read subscriptions", "req", r, "err", err)
		return nil, t_api.NewResonateError(t_api.ErrAIOStoreFailure, "failed to read subscriptions", err)
	}

	util.Assert(completion.Store != nil, "completion must not be nil")

	result := completion.Store.Results[0].ReadSubscriptions
	subscriptions := []*subscription.Subscription{}

	for _, record := range result.Records {
		subscription, err := record.Subscription()
		if err != nil {
			slog.Warn("failed to parse subscription record", "record", record, "err", err)
			continue
		}

		subscriptions = append(subscriptions, subscription)
	}

	// set cursor only if there are more results
	var cursor *t_api.Cursor[t_api.ReadSubscriptionsRequest]
	if result.RowsReturned == int64(r.ReadSubscriptions.Limit) {
		cursor = &t_api.Cursor[t_api.ReadSubscriptionsRequest]{
			Next: &t_api.ReadSubscriptionsRequest{
				PromiseId: r.ReadSubscriptions.PromiseId,
				Limit:     r.ReadSubscriptions.Limit,
				SortId:    &result.LastSortId,
			},
		}
	}

	return &t_api.Response{
		Kind: t_api.ReadSubscriptions,
		Tags: r.Tags,
		ReadSubscriptions: &t_api.ReadSubscriptionsResponse{
			Status:        t_api.StatusOK,
			Cursor:        cursor,
			Subscriptions: subscriptions,
		},
	}, nil
}

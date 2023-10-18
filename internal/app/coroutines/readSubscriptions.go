package coroutines

import (
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/subscription"
)

func ReadSubscriptions(req *t_api.Request, res func(*t_api.Response, error)) *scheduler.Coroutine[*t_aio.Completion, *t_aio.Submission] {
	return scheduler.NewCoroutine("ReadSubscriptions", func(c *scheduler.Coroutine[*t_aio.Completion, *t_aio.Submission]) {
		completion, err := c.Yield(&t_aio.Submission{
			Kind: t_aio.Store,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []*t_aio.Command{
						{
							Kind: t_aio.ReadSubscriptions,
							ReadSubscriptions: &t_aio.ReadSubscriptionsCommand{
								PromiseId: req.ReadSubscriptions.PromiseId,
								Limit:     req.ReadSubscriptions.Limit,
								SortId:    req.ReadSubscriptions.SortId,
							},
						},
					},
				},
			},
		})

		if err != nil {
			slog.Error("failed to read subscriptions", "req", req, "err", err)
			res(nil, err)
			return
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
		if result.RowsReturned == int64(req.ReadSubscriptions.Limit) {
			cursor = &t_api.Cursor[t_api.ReadSubscriptionsRequest]{
				Next: &t_api.ReadSubscriptionsRequest{
					PromiseId: req.ReadSubscriptions.PromiseId,
					Limit:     req.ReadSubscriptions.Limit,
					SortId:    &result.LastSortId,
				},
			}
		}

		res(&t_api.Response{
			Kind: t_api.ReadSubscriptions,
			ReadSubscriptions: &t_api.ReadSubscriptionsResponse{
				Status:        t_api.ResponseOK,
				Cursor:        cursor,
				Subscriptions: subscriptions,
			},
		}, nil)
	})
}

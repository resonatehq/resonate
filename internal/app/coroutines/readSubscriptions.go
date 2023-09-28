package coroutines

import (
	"fmt"
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/subscription"
)

func ReadSubscriptions(t int64, req *types.Request, res func(int64, *types.Response, error)) *scheduler.Coroutine {
	return scheduler.NewCoroutine(fmt.Sprintf("ReadSubscriptions(promiseId=%s)", req.ReadSubscriptions.PromiseId), "ReadSubscriptions", func(s *scheduler.Scheduler, c *scheduler.Coroutine) {
		submission := &types.Submission{
			Kind: types.Store,
			Store: &types.StoreSubmission{
				Transaction: &types.Transaction{
					Commands: []*types.Command{
						{
							Kind: types.StoreReadSubscriptions,
							ReadSubscriptions: &types.ReadSubscriptionsCommand{
								PromiseId: req.ReadSubscriptions.PromiseId,
								Limit:     req.ReadSubscriptions.Limit,
								SortId:    req.ReadSubscriptions.SortId,
							},
						},
					},
				},
			},
		}

		c.Yield(submission, func(t int64, completion *types.Completion, err error) {
			if err != nil {
				slog.Error("failed to read subscriptions", "req", req, "err", err)
				res(t, nil, err)
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
			var cursor *types.Cursor[types.ReadSubscriptionsRequest]
			if result.RowsReturned == int64(req.ReadSubscriptions.Limit) {
				cursor = &types.Cursor[types.ReadSubscriptionsRequest]{
					Next: &types.ReadSubscriptionsRequest{
						PromiseId: req.ReadSubscriptions.PromiseId,
						Limit:     req.ReadSubscriptions.Limit,
						SortId:    &result.LastSortId,
					},
				}
			}

			res(t, &types.Response{
				Kind: types.ReadSubscriptions,
				ReadSubscriptions: &types.ReadSubscriptionsResponse{
					Status:        types.ResponseOK,
					Cursor:        cursor,
					Subscriptions: subscriptions,
				},
			}, nil)
		})
	})
}

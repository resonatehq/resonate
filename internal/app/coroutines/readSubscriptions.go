package coroutines

import (
	"fmt"
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/subscription"
)

func ReadSubscriptions(t int64, req *types.Request, res func(*types.Response, error)) *scheduler.Coroutine {
	return scheduler.NewCoroutine(fmt.Sprintf("ReadSubscriptions(promiseId=%s)", req.ReadSubscriptions.PromiseId), "ReadSubscriptions", func(s *scheduler.Scheduler, c *scheduler.Coroutine) {
		submission := &types.Submission{
			Kind: types.Store,
			Store: &types.StoreSubmission{
				Transaction: &types.Transaction{
					Commands: []*types.Command{
						{
							Kind: types.StoreReadSubscriptions,
							ReadSubscriptions: &types.ReadSubscriptionsCommand{
								PromiseIds: []string{req.ReadSubscriptions.PromiseId},
							},
						},
					},
				},
			},
		}

		c.Yield(submission, func(completion *types.Completion, err error) {
			if err != nil {
				slog.Error("failed to read subscriptions", "req", req, "err", err)
				res(nil, err)
				return
			}

			util.Assert(completion.Store != nil, "completion must not be nil")

			records := completion.Store.Results[0].ReadSubscriptions.Records
			subscriptions := []*subscription.Subscription{}

			for _, record := range records {
				subscription, err := record.Subscription()
				if err != nil {
					slog.Warn("failed to parse subscription record", "record", record, "err", err)
					continue
				}

				subscriptions = append(subscriptions, subscription)
			}

			res(&types.Response{
				Kind: types.ReadSubscriptions,
				ReadSubscriptions: &types.ReadSubscriptionsResponse{
					Status:        types.ResponseOK,
					Subscriptions: subscriptions,
				},
			}, nil)
		})
	})
}

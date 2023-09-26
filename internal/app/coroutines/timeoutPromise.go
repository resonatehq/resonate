package coroutines

import (
	"fmt"
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/subscription"
)

func TimeoutPromise(t int64, p *promise.Promise, subscriptions []*subscription.SubscriptionRecord, retry *scheduler.Coroutine, res func(int64, error)) *scheduler.Coroutine {
	return scheduler.NewCoroutine(fmt.Sprintf("TimeoutPromise(id=%s)", p.Id), "TimeoutPromise", func(s *scheduler.Scheduler, c *scheduler.Coroutine) {
		if subscriptions != nil {
			timeoutPromise(s, c, t, p, subscriptions, retry, res)
		} else {
			submission := &types.Submission{
				Kind: types.Store,
				Store: &types.StoreSubmission{
					Transaction: &types.Transaction{
						Commands: []*types.Command{
							{
								Kind: types.StoreReadSubscriptions,
								ReadSubscriptions: &types.ReadSubscriptionsCommand{
									PromiseIds: []string{p.Id},
								},
							},
						},
					},
				},
			}

			c.Yield(submission, func(t int64, completion *types.Completion, err error) {
				if err != nil {
					slog.Error("failed to read subscriptions", "id", p.Id, "err", err)
					res(t, err)
					return
				}

				util.Assert(completion.Store != nil, "completion must not be nil")
				subscriptions := completion.Store.Results[0].ReadSubscriptions.Records

				timeoutPromise(s, c, t, p, subscriptions, retry, res)
			})
		}
	})
}

func timeoutPromise(s *scheduler.Scheduler, c *scheduler.Coroutine, t int64, p *promise.Promise, subscriptions []*subscription.SubscriptionRecord, retry *scheduler.Coroutine, res func(int64, error)) {
	commands := []*types.Command{
		{
			Kind: types.StoreUpdatePromise,
			UpdatePromise: &types.UpdatePromiseCommand{
				Id:    p.Id,
				State: promise.Timedout,
				Value: promise.Value{
					Headers: map[string]string{},
					Ikey:    nil,
					Data:    []byte{},
				},
				CompletedOn: p.Timeout,
			},
		},
		{
			Kind: types.StoreDeleteTimeout,
			DeleteTimeout: &types.DeleteTimeoutCommand{
				Id: p.Id,
			},
		},
		{
			Kind: types.StoreDeleteSubscriptions,
			DeleteSubscriptions: &types.DeleteSubscriptionsCommand{
				PromiseId: p.Id,
			},
		},
	}

	for _, record := range subscriptions {
		commands = append(commands, &types.Command{
			Kind: types.StoreCreateNotification,
			CreateNotification: &types.CreateNotificationCommand{
				Id:          record.Id,
				PromiseId:   record.PromiseId,
				Url:         record.Url,
				RetryPolicy: record.RetryPolicy,
				Time:        t,
			},
		})
	}

	submission := &types.Submission{
		Kind: types.Store,
		Store: &types.StoreSubmission{
			Transaction: &types.Transaction{
				Commands: commands,
			},
		},
	}

	c.Yield(submission, func(t int64, completion *types.Completion, err error) {
		if err != nil {
			slog.Error("failed to update state", "id", p.Id, "err", err)
			res(t, err)
			return
		}

		util.Assert(completion.Store != nil, "completion must not be nil")

		result := completion.Store.Results[0].UpdatePromise
		util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

		if result.RowsAffected == 1 {
			res(t, nil)
		} else {
			s.Add(retry)
		}
	})
}

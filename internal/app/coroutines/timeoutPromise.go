package coroutines

import (
	"fmt"
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
)

func TimeoutPromise(t int64, p *promise.Promise, retry *scheduler.Coroutine, res func(error)) *scheduler.Coroutine {
	return scheduler.NewCoroutine(fmt.Sprintf("TimeoutPromise(id=%s)", p.Id), "TimeoutPromise", func(s *scheduler.Scheduler, c *scheduler.Coroutine) {
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

		c.Yield(submission, func(completion *types.Completion, err error) {
			if err != nil {
				slog.Error("failed to read subscriptions", "id", p.Id, "err", err)
				res(err)
				return
			}

			util.Assert(completion.Store != nil, "completion must not be nil")
			records := completion.Store.Results[0].ReadSubscriptions.Records

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

			for _, record := range records {
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

			c.Yield(submission, func(completion *types.Completion, err error) {
				if err != nil {
					slog.Error("failed to update state", "id", p.Id, "err", err)
					res(err)
					return
				}

				util.Assert(completion.Store != nil, "completion must not be nil")

				result := completion.Store.Results[0].UpdatePromise
				util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

				if result.RowsAffected == 1 {
					res(nil)
				} else {
					s.Add(retry)
				}
			})
		})
	})
}

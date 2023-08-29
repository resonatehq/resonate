package coroutines

import (
	"fmt"
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
)

func TimeoutPromise(t int64, id string, retry *scheduler.Coroutine, res func(error)) *scheduler.Coroutine {
	return scheduler.NewCoroutine(fmt.Sprintf("TimeoutPromise(id=%s)", id), "TimeoutPromise", func(s *scheduler.Scheduler, c *scheduler.Coroutine) {
		submission := &types.Submission{
			Kind: types.Store,
			Store: &types.StoreSubmission{
				Transaction: &types.Transaction{
					Commands: []*types.Command{
						{
							Kind: types.StoreReadSubscriptions,
							ReadSubscriptions: &types.ReadSubscriptionsCommand{
								PromiseIds: []string{id},
							},
						},
					},
				},
			},
		}

		c.Yield(submission, func(completion *types.Completion, err error) {
			if err != nil {
				slog.Error("failed to read subscriptions", "id", id, "err", err)
				res(err)
				return
			}

			util.Assert(completion.Store != nil, "completion must not be nil")
			records := completion.Store.Results[0].ReadSubscriptions.Records

			commands := []*types.Command{
				{
					Kind: types.StoreUpdatePromise,
					UpdatePromise: &types.UpdatePromiseCommand{
						Id:    id,
						State: promise.Timedout,
						Value: promise.Value{
							Headers: map[string]string{},
							Ikey:    nil,
							Data:    nil,
						},
					},
				},
				{
					Kind: types.StoreDeleteTimeout,
					DeleteTimeout: &types.DeleteTimeoutCommand{
						Id: id,
					},
				},
			}

			for _, record := range records {
				commands = append(commands, &types.Command{
					Kind: types.StoreCreateNotification,
					CreateNotification: &types.CreateNotificationCommand{
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
					slog.Error("failed to update state", "id", id, "err", err)
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

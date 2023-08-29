package coroutines

import (
	"fmt"
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
)

func TimeoutPromises(t int64, cfg *system.Config) *scheduler.Coroutine {
	return scheduler.NewCoroutine(fmt.Sprintf("TimeoutPromises(t=%d)", t), "TimeoutPromises", func(s *scheduler.Scheduler, c *scheduler.Coroutine) {
		submission := &types.Submission{
			Kind: types.Store,
			Store: &types.StoreSubmission{
				Transaction: &types.Transaction{
					Commands: []*types.Command{
						{
							Kind: types.StoreReadTimeouts,
							ReadTimeouts: &types.ReadTimeoutsCommand{
								N: cfg.TimeoutCacheSize,
							},
						},
					},
				},
			},
		}

		c.Yield(submission, func(completion *types.Completion, err error) {
			if err != nil {
				slog.Error("failed to read timeouts", "err", err)
				return
			}

			util.Assert(completion.Store != nil, "completion must not be nil")

			records := completion.Store.Results[0].ReadTimeouts.Records
			promiseIds := []string{}

			for _, record := range records {
				if t >= record.Time {
					promiseIds = append(promiseIds, record.Id)
				}
			}

			if len(promiseIds) > 0 {
				submission := &types.Submission{
					Kind: types.Store,
					Store: &types.StoreSubmission{
						Transaction: &types.Transaction{
							Commands: []*types.Command{
								{
									Kind: types.StoreReadSubscriptions,
									ReadSubscriptions: &types.ReadSubscriptionsCommand{
										PromiseIds: promiseIds,
									},
								},
							},
						},
					},
				}

				c.Yield(submission, func(completion *types.Completion, err error) {
					if err != nil {
						slog.Error("failed to read subscriptions", "err", err)
						return
					}

					util.Assert(completion.Store != nil, "completion must not be nil")

					records := completion.Store.Results[0].ReadSubscriptions.Records
					commands := []*types.Command{}

					for _, promiseId := range promiseIds {
						commands = append(commands, &types.Command{
							Kind: types.StoreUpdatePromise,
							UpdatePromise: &types.UpdatePromiseCommand{
								Id:    promiseId,
								State: promise.Timedout,
								Value: promise.Value{
									Headers: map[string]string{},
									Ikey:    nil,
									Data:    nil,
								},
							},
						}, &types.Command{
							Kind: types.StoreDeleteTimeout,
							DeleteTimeout: &types.DeleteTimeoutCommand{
								Id: promiseId,
							},
						})
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
							slog.Error("failed to update state", "err", err)
							return
						}
					})
				})
			}
		})
	})
}

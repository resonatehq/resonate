package coroutines

import (
	"fmt"
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/resonatehq/resonate/internal/util"
)

func TimeoutPromises(t int64, config *system.Config) *scheduler.Coroutine {
	return scheduler.NewCoroutine(fmt.Sprintf("TimeoutPromises(t=%d)", t), "TimeoutPromises", func(s *scheduler.Scheduler, c *scheduler.Coroutine) {
		submission := &types.Submission{
			Kind: types.Store,
			Store: &types.StoreSubmission{
				Transaction: &types.Transaction{
					Commands: []*types.Command{
						{
							Kind: types.StoreTimeoutCreateNotifications,
							TimeoutCreateNotifications: &types.TimeoutCreateNotificationsCommand{
								Time: t,
							},
						},
						{
							Kind: types.StoreTimeoutDeleteSubscriptions,
							TimeoutDeleteSubscriptions: &types.TimeoutDeleteSubscriptionsCommand{
								Time: t,
							},
						},
						{
							Kind: types.StoreTimeoutPromises,
							TimeoutPromises: &types.TimeoutPromisesCommand{
								Time: t,
							},
						},
					},
				},
			},
		}

		c.Yield(submission, func(t int64, completion *types.Completion, err error) {
			if err != nil {
				slog.Error("failed to read timeouts", "err", err)
				return
			}

			util.Assert(completion.Store != nil, "completion must not be nil")
			util.Assert(len(completion.Store.Results) == 3, "completion must have three results")

			notifications := completion.Store.Results[0].TimeoutCreateNotifications.RowsAffected
			subscriptions := completion.Store.Results[1].TimeoutDeleteSubscriptions.RowsAffected
			promises := completion.Store.Results[2].TimeoutPromises.RowsAffected

			util.Assert(notifications == subscriptions, "must create the same number of notifications as subscriptions deleted")
			if promises == 0 {
				util.Assert(subscriptions == 0 && notifications == 0, "must not create notifications when no promises timed out")
			}
		})
	})
}

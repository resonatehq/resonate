package coroutines

import (
	"fmt"
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/metadata"
	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/util"
)

func TimeoutPromises(t int64, config *system.Config) *scheduler.Coroutine[*t_aio.Completion, *t_aio.Submission] {
	metadata := metadata.New(fmt.Sprintf("tick:%d:timeout", t))
	metadata.Tags.Set("name", "timeout-promises")

	return scheduler.NewCoroutine(metadata, func(c *scheduler.Coroutine[*t_aio.Completion, *t_aio.Submission]) {
		completion, err := c.Yield(&t_aio.Submission{
			Kind: t_aio.Store,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []*t_aio.Command{
						{
							Kind: t_aio.TimeoutCreateNotifications,
							TimeoutCreateNotifications: &t_aio.TimeoutCreateNotificationsCommand{
								Time: c.Time(),
							},
						},
						{
							Kind: t_aio.TimeoutDeleteSubscriptions,
							TimeoutDeleteSubscriptions: &t_aio.TimeoutDeleteSubscriptionsCommand{
								Time: c.Time(),
							},
						},
						{
							Kind: t_aio.TimeoutPromises,
							TimeoutPromises: &t_aio.TimeoutPromisesCommand{
								Time: c.Time(),
							},
						},
					},
				},
			},
		})
		// return nil -- "called using nil *PlatformLevelError pointer"
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
}

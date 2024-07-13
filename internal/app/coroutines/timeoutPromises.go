package coroutines

import (
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/util"
)

func TimeoutPromises(config *system.Config, tags map[string]string) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, any] {
	util.Assert(tags != nil, "tags must be set")

	return func(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any]) (any, error) {
		completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
			Kind: t_aio.Store,
			Tags: tags,
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
		if err != nil {
			slog.Error("failed to read timeouts", "err", err)
			return nil, nil
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

		return nil, nil
	}
}

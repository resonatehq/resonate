package coroutines

import (
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
)

func TimeoutPromise(p *promise.Promise) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, bool] {
	return func(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, bool]) (bool, error) {
		completedState := promise.GetTimedoutState(p)
		util.Assert(completedState == promise.Timedout || completedState == promise.Resolved, "terminus state must be Timedout or Resolved")

		completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
			Kind: t_aio.Store,
			Tags: map[string]string{},
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []*t_aio.Command{
						{
							Kind: t_aio.UpdatePromise,
							UpdatePromise: &t_aio.UpdatePromiseCommand{
								Id:    p.Id,
								State: completedState,
								Value: promise.Value{
									Headers: map[string]string{},
									Data:    []byte{},
								},
								CompletedOn: p.Timeout,
							},
						},
						{
							Kind: t_aio.CreateNotifications,
							CreateNotifications: &t_aio.CreateNotificationsCommand{
								PromiseId: p.Id,
								Time:      c.Time(),
							},
						},
						{
							Kind: t_aio.DeleteSubscriptions,
							DeleteSubscriptions: &t_aio.DeleteSubscriptionsCommand{
								PromiseId: p.Id,
							},
						},
					},
				},
			},
		})

		if err != nil {
			slog.Error("failed to update promise", "id", p.Id, "err", err)
			return false, err
		}

		util.Assert(completion.Store != nil, "completion must not be nil")

		result := completion.Store.Results[0].UpdatePromise
		util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

		return result.RowsAffected == 1, nil
	}
}

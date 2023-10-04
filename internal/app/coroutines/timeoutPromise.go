package coroutines

import (
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
)

func TimeoutPromise(t int64, p *promise.Promise, retry *scheduler.Coroutine, res func(int64, error)) *scheduler.Coroutine {
	return scheduler.NewCoroutine("TimeoutPromise", func(s *scheduler.Scheduler, c *scheduler.Coroutine) {
		submission := &t_aio.Submission{
			Kind: t_aio.Store,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []*t_aio.Command{
						{
							Kind: t_aio.UpdatePromise,
							UpdatePromise: &t_aio.UpdatePromiseCommand{
								Id:    p.Id,
								State: promise.Timedout,
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
								Time:      t,
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
		}

		c.Yield(submission, func(t int64, completion *t_aio.Completion, err error) {
			if err != nil {
				slog.Error("failed to update promise", "id", p.Id, "err", err)
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
	})
}

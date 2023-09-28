package coroutines

import (
	"fmt"
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
)

func TimeoutPromise(t int64, p *promise.Promise, retry *scheduler.Coroutine, res func(int64, error)) *scheduler.Coroutine {
	return scheduler.NewCoroutine(fmt.Sprintf("TimeoutPromise(id=%s)", p.Id), "TimeoutPromise", func(s *scheduler.Scheduler, c *scheduler.Coroutine) {
		submission := &types.Submission{
			Kind: types.Store,
			Store: &types.StoreSubmission{
				Transaction: &types.Transaction{
					Commands: []*types.Command{
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
							Kind: types.StoreCreateNotifications,
							CreateNotifications: &types.CreateNotificationsCommand{
								PromiseId: p.Id,
								Time:      t,
							},
						},
						{
							Kind: types.StoreDeleteSubscriptions,
							DeleteSubscriptions: &types.DeleteSubscriptionsCommand{
								PromiseId: p.Id,
							},
						},
					},
				},
			},
		}

		c.Yield(submission, func(t int64, completion *types.Completion, err error) {
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

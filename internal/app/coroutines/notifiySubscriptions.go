package coroutines

import (
	"encoding/json"
	"net/http"

	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/notification"
)

var inflights = inflight{}

type inflight map[int64]bool

func (i inflight) get(id int64) bool {
	return i[id]
}

func (i inflight) add(id int64) {
	i[id] = true
}

func (i inflight) remove(id int64) {
	delete(i, id)
}

func NotifySubscriptions(t int64, cfg *system.Config) *scheduler.Coroutine {
	return scheduler.NewCoroutine(func(s *scheduler.Scheduler, c *scheduler.Coroutine) {
		submission := &types.Submission{
			Kind: types.Store,
			Store: &types.StoreSubmission{
				Transaction: &types.Transaction{
					Commands: []*types.Command{
						{
							Kind: types.StoreReadNotifications,
							ReadNotifications: &types.ReadNotificationsCommand{
								N: cfg.NotificationCacheSize,
							},
						},
					},
				},
			},
		}

		c.Yield(submission, func(completion *types.Completion, err error) {
			if err != nil {
				// TODO: log
				return
			}

			util.Assert(completion.Store != nil, "completion must not be nil")
			records := completion.Store.Results[0].ReadNotifications.Records

			for _, record := range records {
				if t >= record.Time && !inflights.get(record.Id) {
					s.Add(notifySubscription(record))
				}
			}
		})
	})
}

func notifySubscription(record *notification.NotificationRecord) *scheduler.Coroutine {
	return scheduler.NewCoroutine(func(s *scheduler.Scheduler, c *scheduler.Coroutine) {
		// handle inflight cache
		inflights.add(record.Id)
		c.OnDone(func() { inflights.remove(record.Id) })

		submission := &types.Submission{
			Kind: types.Store,
			Store: &types.StoreSubmission{
				Transaction: &types.Transaction{
					Commands: []*types.Command{
						{
							ReadPromise: &types.ReadPromiseCommand{
								Id: record.PromiseId,
							},
						},
					},
				},
			},
		}

		c.Yield(submission, func(completion *types.Completion, err error) {
			if err != nil {
				// TODO: log (recoverable)
				return
			}

			util.Assert(completion.Store != nil, "completion must not be nil")

			result := completion.Store.Results[0].ReadPromise
			util.Assert(result.RowsReturned == 0 || result.RowsReturned == 1, "result must return 0 or 1 rows")

			if result.RowsReturned == 0 {
				// TODO: log
				abort(c, record)
				return
			}

			promise, err := result.Records[0].Promise()
			if err != nil {
				// TODO: log
				abort(c, record)
				return
			}

			body, err := json.Marshal(promise)
			if err != nil {
				// TODO: log
				abort(c, record)
				return
			}

			submission := &types.Submission{
				Kind: types.Network,
				Network: &types.NetworkSubmission{
					Http: &types.HttpRequest{
						Method: "POST",
						Url:    record.Url,
						Body:   body,
					},
				},
			}

			c.Yield(submission, func(completion *types.Completion, err error) {
				var command *types.Command

				if backoff, ok := util.Backoff(record.Attempt); ok && (err != nil || !isSuccessful(completion.Network.Http)) {
					command = &types.Command{
						Kind: types.StoreUpdateNotification,
						UpdateNotification: &types.UpdateNotificationCommand{
							Id:      record.Id,
							Time:    record.Time + backoff.Milliseconds(),
							Attempt: record.Attempt + 1,
						},
					}
				} else {
					command = &types.Command{
						Kind: types.StoreDeleteNotification,
						DeleteNotification: &types.DeleteNotificationCommand{
							Id: record.Id,
						},
					}
				}

				submission := &types.Submission{
					Kind: types.Store,
					Store: &types.StoreSubmission{
						Transaction: &types.Transaction{
							Commands: []*types.Command{command},
						},
					},
				}

				c.Yield(submission, func(completion *types.Completion, err error) {
					// if err != nil {
					// 	// TODO: log
					// }
				})
			})
		})
	})
}

func abort(c *scheduler.Coroutine, record *notification.NotificationRecord) {
	submission := &types.Submission{
		Kind: types.Store,
		Store: &types.StoreSubmission{
			Transaction: &types.Transaction{
				Commands: []*types.Command{
					{
						Kind: types.StoreDeleteNotification,
						DeleteNotification: &types.DeleteNotificationCommand{
							Id: record.Id,
						},
					},
				},
			},
		},
	}

	c.Yield(submission, func(completion *types.Completion, err error) {
		// if err != nil {
		// 	// TODO: log
		// }
	})
}

func isSuccessful(res *http.Response) bool {
	// svix only checks for 2xx response codes and retries under all
	// other circumstances
	return res.StatusCode >= 200 && res.StatusCode < 300
}

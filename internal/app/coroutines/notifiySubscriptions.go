package coroutines

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/notification"
)

var notificationsInflight = inflight{}

type inflight map[string]bool

func (i inflight) get(id string) bool {
	return i[id]
}

func (i inflight) add(id string) {
	i[id] = true
}

func (i inflight) remove(id string) {
	delete(i, id)
}

func NotifySubscriptions(config *system.Config, tags map[string]string) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, any] {
	util.Assert(tags != nil, "tags must be set")

	return func(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any]) (any, error) {
		completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
			Kind: t_aio.Store,
			Tags: tags,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []*t_aio.Command{
						{
							Kind: t_aio.ReadNotifications,
							ReadNotifications: &t_aio.ReadNotificationsCommand{
								N: config.NotificationCacheSize,
							},
						},
					},
				},
			},
		})

		if err != nil {
			slog.Error("failed to read notifications", "err", err)
			return nil, nil
		}

		util.Assert(completion.Store != nil, "completion must not be nil")
		records := completion.Store.Results[0].ReadNotifications.Records

		for _, record := range records {
			notification, err := record.Notification()
			if err != nil {
				slog.Warn("failed to parse notification record", "record", record, "err", err)
				continue
			}

			if c.Time() >= record.Time && !notificationsInflight.get(id(notification)) {
				notificationsInflight.add(id(notification))

				// TODO: fix runaway concurrency
				gocoro.Spawn(c, notifySubscription(notification, tags))
			}
		}

		return nil, nil
	}
}

func notifySubscription(notification *notification.Notification, tags map[string]string) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, any] {
	return func(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any]) (any, error) {
		defer notificationsInflight.remove(id(notification))

		completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
			Kind: t_aio.Store,
			Tags: tags,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []*t_aio.Command{
						{
							Kind: t_aio.ReadPromise,
							ReadPromise: &t_aio.ReadPromiseCommand{
								Id: notification.PromiseId,
							},
						},
					},
				},
			},
		})

		if err != nil {
			slog.Error("failed to read promise", "id", notification.PromiseId, "err", err)
			return nil, nil
		}

		util.Assert(completion.Store != nil, "completion must not be nil")

		result := completion.Store.Results[0].ReadPromise
		util.Assert(result.RowsReturned == 0 || result.RowsReturned == 1, "result must return 0 or 1 rows")

		if result.RowsReturned == 0 {
			slog.Warn("promise not found, aborting notification", "id", notification.PromiseId)
			abort(c, notification, tags)
			return nil, nil
		}

		record := result.Records[0]
		promise, err := record.Promise()
		if err != nil {
			slog.Warn("failed to parse promise record, aborting notification", "record", record)
			abort(c, notification, tags)
			return nil, nil
		}

		body, err := json.Marshal(promise)
		if err != nil {
			slog.Warn("failed to serialize promise, aborting notification", "promise", promise)
			abort(c, notification, tags)
			return nil, nil
		}

		completion, err = gocoro.YieldAndAwait(c, &t_aio.Submission{
			Kind: t_aio.Network,
			Tags: tags,
			Network: &t_aio.NetworkSubmission{
				Kind: t_aio.Http,
				Http: &t_aio.HttpRequest{
					Method: "POST",
					Url:    notification.Url,
					Body:   body,
				},
			},
		})

		if err != nil {
			slog.Warn("failed to send notification", "promise", promise, "url", notification.Url)
		}

		var command *t_aio.Command
		if (err != nil || !isSuccessful(completion.Network.Http)) && notification.Attempt < notification.RetryPolicy.Attempts {
			command = &t_aio.Command{
				Kind: t_aio.UpdateNotification,
				UpdateNotification: &t_aio.UpdateNotificationCommand{
					Id:        notification.Id,
					PromiseId: notification.PromiseId,
					Time:      backoff(notification.RetryPolicy.Delay, notification.Attempt),
					Attempt:   notification.Attempt + 1,
				},
			}
		} else {
			command = &t_aio.Command{
				Kind: t_aio.DeleteNotification,
				DeleteNotification: &t_aio.DeleteNotificationCommand{
					Id:        notification.Id,
					PromiseId: notification.PromiseId,
				},
			}
		}

		_, err = gocoro.YieldAndAwait(c, &t_aio.Submission{
			Kind: t_aio.Store,
			Tags: tags,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []*t_aio.Command{command},
				},
			},
		})

		if err != nil {
			slog.Warn("failed to update notification", "notification", notification)
		}

		return nil, nil
	}
}

func abort(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], notification *notification.Notification, tags map[string]string) {
	_, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
		Kind: t_aio.Store,
		Tags: tags,
		Store: &t_aio.StoreSubmission{
			Transaction: &t_aio.Transaction{
				Commands: []*t_aio.Command{
					{
						Kind: t_aio.DeleteNotification,
						DeleteNotification: &t_aio.DeleteNotificationCommand{
							Id:        notification.Id,
							PromiseId: notification.PromiseId,
						},
					},
				},
			},
		},
	})

	if err != nil {
		slog.Warn("failed to delete notification", "notification", notification)
	}
}

func id(notification *notification.Notification) string {
	return fmt.Sprintf("%s:%s", notification.Id, notification.PromiseId)
}

func isSuccessful(res *http.Response) bool {
	// svix only checks for 2xx response codes and retries under all
	// other circumstances
	return res.StatusCode >= 200 && res.StatusCode < 300
}

func backoff(delay int64, attempt int64) int64 {
	util.Assert(delay >= 0, "delay must be non-negative")
	util.Assert(attempt >= 0, "delay must be non-negative")

	return delay * int64(math.Pow(2, float64(attempt)))
}

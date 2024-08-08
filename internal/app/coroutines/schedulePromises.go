package coroutines

import (
	"fmt"
	"html/template"
	"log/slog"
	"strings"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/idempotency"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/schedule"
)

var schedulesInflight = inflight{}

func SchedulePromises(config *system.Config, tags map[string]string) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, any] {
	util.Assert(tags != nil, "tags must be set")

	return func(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any]) (any, error) {
		// Read schedules from the store that are due to run.
		completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
			Kind: t_aio.Store,
			Tags: tags,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []*t_aio.Command{
						{
							Kind: t_aio.ReadSchedules,
							ReadSchedules: &t_aio.ReadSchedulesCommand{
								NextRunTime: c.Time(),
								Limit:       config.ScheduleBatchSize,
							},
						},
					},
				},
			},
		})
		if err != nil {
			slog.Error("failed to read schedules", "err", err)
			return nil, nil
		}

		util.Assert(completion.Store != nil, "completion must not be nil")
		records := completion.Store.Results[0].ReadSchedules.Records

		// Schedule child coroutine for all the schedules returned.

		for _, record := range records {
			schedule, err := record.Schedule()
			if err != nil {
				slog.Warn("failed to parse schedule", "err", err)
				continue
			}

			if !schedulesInflight.get(sid(schedule)) {
				schedulesInflight.add(sid(schedule))

				// TODO: fix runaway concurrency
				gocoro.Spawn(c, schedulePromise(schedule, tags))
			}
		}

		return nil, nil
	}
}

func schedulePromise(schedule *schedule.Schedule, tags map[string]string) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, any] {
	// handle creating promise (schedule run) and updating schedule record.

	return func(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any]) (any, error) {
		defer schedulesInflight.remove(sid(schedule))

		next, err := util.Next(schedule.NextRunTime, schedule.Cron)
		if err != nil {
			slog.Error("failed to calculate next run time", "err", err)
			return nil, nil
		}

		id, err := generatePromiseId(schedule.PromiseId, map[string]string{
			"id":        schedule.Id,
			"timestamp": fmt.Sprintf("%d", schedule.NextRunTime),
		})
		if err != nil {
			slog.Error("failed to generate promise id", "err", err)
			return nil, nil
		}

		if schedule.PromiseParam.Headers == nil {
			schedule.PromiseParam.Headers = map[string]string{}
		}
		if schedule.PromiseParam.Data == nil {
			schedule.PromiseParam.Data = []byte{}
		}
		if schedule.PromiseTags == nil {
			schedule.PromiseTags = map[string]string{}
		}

		// add invocation tag
		schedule.PromiseTags["resonate:invocation"] = "true"
		schedule.PromiseTags["resonate:schedule"] = schedule.Id

		// calculate timeout for promise

		state := promise.Pending
		if c.Time() >= schedule.PromiseTimeout+schedule.NextRunTime {
			state = promise.Timedout
		}

		// set promise idempotency key to the promise id,
		// this way the key is non nil and we can
		// "create" idempotently when picked up by the sdk
		idempotencyKey := idempotency.Key(id)

		completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
			Kind: t_aio.Store,
			Tags: tags,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []*t_aio.Command{
						{
							Kind: t_aio.CreatePromise,
							CreatePromise: &t_aio.CreatePromiseCommand{
								Id:             id,
								IdempotencyKey: &idempotencyKey,
								State:          state,
								Param:          schedule.PromiseParam,
								Timeout:        schedule.NextRunTime + schedule.PromiseTimeout,
								Tags:           schedule.PromiseTags,
								CreatedOn:      schedule.NextRunTime,
							},
						},
						{
							Kind: t_aio.UpdateSchedule,
							UpdateSchedule: &t_aio.UpdateScheduleCommand{
								Id:          schedule.Id,
								LastRunTime: &schedule.NextRunTime,
								NextRunTime: next,
							},
						},
					},
				},
			},
		})
		if err != nil {
			slog.Error("failed to create promise and update schedule", "err", err)
			return nil, nil
		}

		util.Assert(completion.Store != nil, "completion must not be nil")

		result := completion.Store.Results[0].CreatePromise
		util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

		if completion.Store.Results[0].CreatePromise.RowsAffected == 0 {
			slog.Warn("promise to be scheduled already exists", "schedule", schedule, "promise", id)
			return nil, nil
		}

		return nil, nil
	}
}

// helpers

func generatePromiseId(id string, vars map[string]string) (string, error) {
	t := template.Must(template.New("promiseId").Parse(id))

	var replaced strings.Builder
	if err := t.Execute(&replaced, vars); err != nil {
		return "", err
	}

	return replaced.String(), nil
}

func sid(schedule *schedule.Schedule) string {
	return fmt.Sprintf("%s:%d", schedule.Id, schedule.NextRunTime)
}

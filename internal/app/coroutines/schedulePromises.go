package coroutines

import (
	"fmt"
	"html/template"
	"log/slog"
	"strings"

	"github.com/resonatehq/resonate/internal/kernel/metadata"
	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/schedule"
)

func SchedulePromises(t int64, config *system.Config) *Coroutine {
	metadata := metadata.New(fmt.Sprintf("tick:%d:schedule", t))
	metadata.Tags.Set("name", "schedule-promises")

	return scheduler.NewCoroutine(metadata, func(c *Coroutine) {

		// Read schedules from the store that are due to run.

		completion, err := c.Yield(&t_aio.Submission{
			Kind: t_aio.Store,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []*t_aio.Command{
						{
							Kind: t_aio.ReadSchedules,
							ReadSchedules: &t_aio.ReadSchedulesCommand{
								NextRunTime: c.Time(),
							},
						},
					},
				},
			},
		})
		if err != nil {
			slog.Error("failed to read schedules", "err", err)
			return
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

			c.Scheduler.Add(schedulePromise(metadata.TransactionId, schedule))
		}
	})
}

func schedulePromise(tid string, schedule *schedule.Schedule) *scheduler.Coroutine[*t_aio.Completion, *t_aio.Submission] {
	metadata := metadata.New(tid)
	metadata.Tags.Set("name", "schedule-promise")

	// handle creating promise (schedule run) and updating schedule record.

	return scheduler.NewCoroutine(metadata, func(c *scheduler.Coroutine[*t_aio.Completion, *t_aio.Submission]) {
		crontime := schedule.NextRunTime
		next, err := util.Next(crontime, schedule.Cron)
		if err != nil {
			slog.Error("failed to calculate next run time", "err", err)
			return
		}

		id, err := generatePromiseId(schedule.PromiseId, map[string]string{
			"timestamp": fmt.Sprintf("%d", crontime),
		})
		if err != nil {
			slog.Error("failed to generate promise id", "err", err)
			return
		}

		if schedule.PromiseParam.Headers == nil {
			schedule.PromiseParam.Headers = map[string]string{}
		}
		if schedule.PromiseParam.Data == nil {
			schedule.PromiseParam.Data = []byte{}
		}

		// calculate timeout for promise

		now := c.Time()
		state := promise.Pending
		if schedule.PromiseTimeout+schedule.NextRunTime < now {
			state = promise.Timedout
		}

		completion, err := c.Yield(&t_aio.Submission{
			Kind: t_aio.Store,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []*t_aio.Command{
						{
							Kind: t_aio.CreatePromise,
							CreatePromise: &t_aio.CreatePromiseCommand{
								Id:             id,
								State:          state,
								Param:          schedule.PromiseParam,
								Timeout:        crontime + schedule.PromiseTimeout,
								IdempotencyKey: nil,
								Tags: map[string]string{
									"resonate:invocation": "true",
								},
								CreatedOn: crontime,
							},
						},
						{
							Kind: t_aio.UpdateSchedule,
							UpdateSchedule: &t_aio.UpdateScheduleCommand{
								Id:          schedule.Id,
								LastRunTime: &crontime,
								NextRunTime: next,
							},
						},
					},
				},
			},
		})
		if err != nil {
			slog.Error("failed to create promise and update schedule", "err", err)
			return
		}

		util.Assert(completion.Store != nil, "completion must not be nil")

		result := completion.Store.Results[0].CreatePromise
		util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

		if completion.Store.Results[0].CreatePromise.RowsAffected == 0 {
			slog.Warn("promise to be scheduled already exists", "schedule", schedule, "promise", id)
			return
		}

	})
}

// helpers

func generatePromiseId(id string, vars map[string]string) (string, error) {
	t := template.Must(template.New("promiseID").Parse(id))

	var replaced strings.Builder
	if err := t.Execute(&replaced, vars); err != nil {
		return "", err
	}

	return replaced.String(), nil
}

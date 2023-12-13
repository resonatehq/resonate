package coroutines

import (
	"fmt"
	"log/slog"

	"github.com/google/uuid"
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
		now := c.Time()
		next, err := util.Next(now, schedule.Cron)
		if err != nil {
			slog.Error("failed to calculate next run time", "err", err)
			return
		}

		_, err = c.Yield(&t_aio.Submission{
			Kind: t_aio.Store,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []*t_aio.Command{
						{

							Kind: t_aio.CreatePromise,
							CreatePromise: &t_aio.CreatePromiseCommand{
								Id:             "schedule-" + uuid.NewString(),
								Param:          promise.Value{},
								Timeout:        100000000,
								IdempotencyKey: nil,
								Tags:           map[string]string{},
								CreatedOn:      now,
							},
						},
						{
							Kind: t_aio.UpdateSchedule,
							UpdateSchedule: &t_aio.UpdateScheduleCommand{
								Id:          schedule.Id,
								Desc:        schedule.Desc,
								Cron:        schedule.Cron,
								LastRunTime: &now,
								NextRunTime: next,
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

		// todo: add checks for promise creation failure and schedule update failure.

	})
}

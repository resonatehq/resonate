package coroutines

import (
	"fmt"
	"html/template"
	"log/slog"
	"strings"

	"github.com/resonatehq/gocoro"
	gocoroPromise "github.com/resonatehq/gocoro/pkg/promise"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/util"
)

func SchedulePromises(config *system.Config, metadata map[string]string) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, any] {
	util.Assert(metadata != nil, "metadata must be set")

	return func(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any]) (any, error) {
		// read elapsed schedules
		completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
			Kind: t_aio.Store,
			Tags: metadata,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []t_aio.Command{
						&t_aio.ReadSchedulesCommand{
							NextRunTime: c.Time(),
							Limit:       config.ScheduleBatchSize,
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
		util.Assert(len(completion.Store.Results) == 1, "completion must have one result")

		result := t_aio.AsQuerySchedules(completion.Store.Results[0])
		util.Assert(result != nil, "result must not be nil")

		awaiting := make([]gocoroPromise.Awaitable[*t_aio.Completion], len(result.Records))
		commands := make([]*t_aio.CreatePromiseCommand, len(result.Records))

		for i, r := range result.Records {
			util.Assert(r.NextRunTime <= c.Time(), "schedule next run time must have elapsed")

			s, err := r.Schedule()
			if err != nil {
				slog.Error("failed to parse schedule, skipping", "err", err)
				continue
			}

			next, err := util.Next(s.NextRunTime, s.Cron)
			if err != nil {
				slog.Error("failed to calculate next run time for schedule, skipping", "err", err)
				continue
			}

			id, err := generatePromiseId(s.PromiseId, map[string]string{
				"id":        s.Id,
				"timestamp": fmt.Sprintf("%d", s.NextRunTime),
			})
			if err != nil {
				slog.Error("failed to generate promise id for scheduled promise, skipping", "err", err)
				continue
			}

			// add invocation tags
			s.PromiseTags["resonate:schedule"] = s.Id
			s.PromiseTags["resonate:invocation"] = "true"

			// create promise command
			commands[i] = &t_aio.CreatePromiseCommand{
				Id:        id,
				Param:     s.PromiseParam,
				Timeout:   s.PromiseTimeout + s.NextRunTime,
				Tags:      s.PromiseTags,
				CreatedOn: c.Time(),
			}

			awaiting[i] = gocoro.Spawn(c, createPromise(metadata, commands[i], nil, &t_aio.UpdateScheduleCommand{
				Id:          s.Id,
				LastRunTime: &s.NextRunTime,
				NextRunTime: next,
			}))
		}

		for i := 0; i < len(awaiting); i++ {
			if awaiting[i] == nil {
				continue
			}

			completion, err := gocoro.Await(c, awaiting[i])
			if err != nil {
				slog.Error("failed to schedule promise", "err", err)
				continue
			}

			createPromiseRes := t_aio.AsAlterPromises(completion.Store.Results[0])

			if createPromiseRes.RowsAffected == 0 {
				slog.Warn("promise to be scheduled already exists", "promise", commands[i].Id, "schedule", result.Records[i].Id)
			}
		}

		return nil, nil
	}
}

// Helper functions

func generatePromiseId(id string, vars map[string]string) (string, error) {
	t := template.Must(template.New("promiseId").Parse(id))

	var replaced strings.Builder
	if err := t.Execute(&replaced, vars); err != nil {
		return "", err
	}

	return replaced.String(), nil
}

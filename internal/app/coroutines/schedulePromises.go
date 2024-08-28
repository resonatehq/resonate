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
	"github.com/resonatehq/resonate/pkg/idempotency"
	"github.com/resonatehq/resonate/pkg/promise"
)

func SchedulePromises(config *system.Config, tags map[string]string) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, any] {
	util.Assert(tags != nil, "tags must be set")

	return func(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any]) (any, error) {
		// read elapsed schedules
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
		util.Assert(len(completion.Store.Results) == 1, "completion must have one result")

		result := completion.Store.Results[0].ReadSchedules
		util.Assert(result != nil, "result must not be nil")

		awaiting := make([]gocoroPromise.Awaitable[*t_aio.Completion], len(result.Records))

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

			if s.PromiseParam.Headers == nil {
				s.PromiseParam.Headers = map[string]string{}
			}
			if s.PromiseParam.Data == nil {
				s.PromiseParam.Data = []byte{}
			}
			if s.PromiseTags == nil {
				s.PromiseTags = map[string]string{}
			}

			// add invocation tag
			s.PromiseTags["resonate:invocation"] = "true"
			s.PromiseTags["resonate:schedule"] = s.Id

			// calculate state
			state := promise.Pending
			if c.Time() >= s.PromiseTimeout+s.NextRunTime {
				state = promise.Timedout
			}

			// set promise idempotency key to the promise id,
			// this way the key is non nil and we can
			// "create" idempotently when picked up by the sdk
			idempotencyKey := idempotency.Key(id)

			awaiting[i] = gocoro.Yield(c, &t_aio.Submission{
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
									Param:          s.PromiseParam,
									Timeout:        s.PromiseTimeout + s.NextRunTime,
									Tags:           s.PromiseTags,
									CreatedOn:      s.NextRunTime,
								},
							},
							{
								Kind: t_aio.UpdateSchedule,
								UpdateSchedule: &t_aio.UpdateScheduleCommand{
									Id:          s.Id,
									LastRunTime: &s.NextRunTime,
									NextRunTime: next,
								},
							},
						},
					},
				},
			})
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

			util.Assert(completion.Store != nil, "completion must not be nil")
			util.Assert(len(completion.Store.Results) == 2, "completion must have two results")

			results := completion.Store.Results
			util.Assert(results[0].CreatePromise.RowsAffected == 0 || results[0].CreatePromise.RowsAffected == 1, "result must return 0 or 1 rows")
			util.Assert(results[1].UpdateSchedule.RowsAffected == 0 || results[1].UpdateSchedule.RowsAffected == 1, "result must return 0 or 1 rows")
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

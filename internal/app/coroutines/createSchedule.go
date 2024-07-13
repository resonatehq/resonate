package coroutines

import (
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/schedule"
)

func CreateSchedule(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	if r.CreateSchedule.Tags == nil {
		r.CreateSchedule.Tags = map[string]string{}
	}
	if r.CreateSchedule.PromiseParam.Headers == nil {
		r.CreateSchedule.PromiseParam.Headers = map[string]string{}
	}
	if r.CreateSchedule.PromiseParam.Data == nil {
		r.CreateSchedule.PromiseParam.Data = []byte{}
	}
	if r.CreateSchedule.PromiseTags == nil {
		r.CreateSchedule.PromiseTags = map[string]string{}
	}

	completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
		Kind: t_aio.Store,
		Tags: r.Tags,
		Store: &t_aio.StoreSubmission{
			Transaction: &t_aio.Transaction{
				Commands: []*t_aio.Command{
					{
						Kind: t_aio.ReadSchedule,
						ReadSchedule: &t_aio.ReadScheduleCommand{
							Id: r.CreateSchedule.Id,
						},
					},
				},
			},
		},
	})
	if err != nil {
		slog.Error("failed to read schedule", "req", r, "err", err)
		return nil, t_api.NewResonateError(t_api.ErrAIOStoreFailure, "failed to read schedule", err)
	}

	util.Assert(completion.Store != nil, "completion must not be nil")
	result := completion.Store.Results[0].ReadSchedule
	util.Assert(result.RowsReturned == 0 || result.RowsReturned == 1, "result must return 0 or 1 rows")

	var res *t_api.Response

	if result.RowsReturned == 0 {
		createdOn := c.Time()
		next, err := util.Next(createdOn, r.CreateSchedule.Cron)
		if err != nil {
			slog.Error("failed to calculate next run time", "req", r, "err", err)
			return nil, t_api.NewResonateError(t_api.ErrAIOStoreSerializationFailure, "failed to calculate next run time", err)
		}

		completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
			Kind: t_aio.Store,
			Tags: r.Tags,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []*t_aio.Command{
						{
							Kind: t_aio.CreateSchedule,
							CreateSchedule: &t_aio.CreateScheduleCommand{
								Id:             r.CreateSchedule.Id,
								Description:    r.CreateSchedule.Description,
								Cron:           r.CreateSchedule.Cron,
								Tags:           r.CreateSchedule.Tags,
								PromiseId:      r.CreateSchedule.PromiseId,
								PromiseTimeout: r.CreateSchedule.PromiseTimeout,
								PromiseParam:   r.CreateSchedule.PromiseParam,
								PromiseTags:    r.CreateSchedule.PromiseTags,
								NextRunTime:    next,
								IdempotencyKey: r.CreateSchedule.IdempotencyKey,
								CreatedOn:      createdOn,
							},
						},
					},
				},
			},
		})
		if err != nil {
			slog.Error("failed to create schedule", "req", r, "err", err)
			return nil, t_api.NewResonateError(t_api.ErrAIOStoreFailure, "failed to create schedule", err)
		}

		util.Assert(completion.Store != nil, "completion must not be nil")
		result := completion.Store.Results[0].CreateSchedule
		util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

		if result.RowsAffected == 1 {
			res = &t_api.Response{
				Kind: t_api.CreateSchedule,
				Tags: r.Tags,
				CreateSchedule: &t_api.CreateScheduleResponse{
					Status: t_api.StatusCreated,
					Schedule: &schedule.Schedule{
						Id:             r.CreateSchedule.Id,
						Description:    r.CreateSchedule.Description,
						Cron:           r.CreateSchedule.Cron,
						Tags:           r.CreateSchedule.Tags,
						PromiseId:      r.CreateSchedule.PromiseId,
						PromiseTimeout: r.CreateSchedule.PromiseTimeout,
						PromiseParam:   r.CreateSchedule.PromiseParam,
						PromiseTags:    r.CreateSchedule.PromiseTags,
						LastRunTime:    nil,
						NextRunTime:    next,
						IdempotencyKey: r.CreateSchedule.IdempotencyKey,
						CreatedOn:      createdOn,
					},
				},
			}
		} else {
			// It's possible that the schedule was completed by another coroutine
			// while we were creating. In that case, we should just retry.
			return CreateSchedule(c, r)
		}
	} else {
		// return resource conflict.

		s, err := result.Records[0].Schedule()
		if err != nil {
			slog.Error("failed to parse schedule", "req", r, "err", err)
			return nil, t_api.NewResonateError(t_api.ErrAIOStoreFailure, "failed to parse schedule", err)
		}

		status := t_api.StatusScheduleAlreadyExists
		if s.IdempotencyKey.Match(r.CreateSchedule.IdempotencyKey) {
			status = t_api.StatusOK
		}

		res = &t_api.Response{
			Kind: t_api.CreateSchedule,
			Tags: r.Tags,
			CreateSchedule: &t_api.CreateScheduleResponse{
				Status:   status,
				Schedule: s,
			},
		}
	}

	util.Assert(res != nil, "response must not be nil")
	return res, nil
}

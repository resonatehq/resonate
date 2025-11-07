package coroutines

import (
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/schedule"
)

func CreateSchedule(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	req := r.Payload.(*t_api.CreateScheduleRequest)
	if req.Tags == nil {
		req.Tags = map[string]string{}
	}
	if req.PromiseParam.Headers == nil {
		req.PromiseParam.Headers = map[string]string{}
	}
	if req.PromiseParam.Data == nil {
		req.PromiseParam.Data = []byte{}
	}
	if req.PromiseTags == nil {
		req.PromiseTags = map[string]string{}
	}

	metrics := c.Get("metrics").(*metrics.Metrics)

	completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
		Kind: t_aio.Store,
		Tags: r.Metadata,
		Store: &t_aio.StoreSubmission{
			Transaction: &t_aio.Transaction{
				Commands: []t_aio.Command{
					&t_aio.ReadScheduleCommand{
						Id: req.Id,
					},
				},
			},
		},
	})
	if err != nil {
		slog.Error("failed to read schedule", "req", r, "err", err)
		return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
	}

	util.Assert(completion.Store != nil, "completion must not be nil")
	result := t_aio.AsQuerySchedules(completion.Store.Results[0])
	util.Assert(result.RowsReturned == 0 || result.RowsReturned == 1, "result must return 0 or 1 rows")

	var res *t_api.Response

	if result.RowsReturned == 0 {
		createdOn := c.Time()
		next, err := util.Next(createdOn, req.Cron)
		if err != nil {
			slog.Error("failed to calculate next run time", "req", r, "err", err)
			return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
		}

		completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
			Kind: t_aio.Store,
			Tags: r.Metadata,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []t_aio.Command{
						&t_aio.CreateScheduleCommand{
							Id:             req.Id,
							Description:    req.Description,
							Cron:           req.Cron,
							Tags:           req.Tags,
							PromiseId:      req.PromiseId,
							PromiseTimeout: req.PromiseTimeout,
							PromiseParam:   req.PromiseParam,
							PromiseTags:    req.PromiseTags,
							NextRunTime:    next,
							IdempotencyKey: req.IdempotencyKey,
							CreatedOn:      createdOn,
						},
					},
				},
			},
		})
		if err != nil {
			slog.Error("failed to create schedule", "req", r, "err", err)
			return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
		}

		util.Assert(completion.Store != nil, "completion must not be nil")
		result := t_aio.AsAlterSchedules(completion.Store.Results[0])
		util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

		if result.RowsAffected == 1 {
			res = &t_api.Response{
				Status:   t_api.StatusCreated,
				Metadata: r.Metadata,
				Payload: &t_api.CreateScheduleResponse{
					Schedule: &schedule.Schedule{
						Id:             req.Id,
						Description:    req.Description,
						Cron:           req.Cron,
						Tags:           req.Tags,
						PromiseId:      req.PromiseId,
						PromiseTimeout: req.PromiseTimeout,
						PromiseParam:   req.PromiseParam,
						PromiseTags:    req.PromiseTags,
						LastRunTime:    nil,
						NextRunTime:    next,
						IdempotencyKey: req.IdempotencyKey,
						CreatedOn:      createdOn,
					},
				},
			}

			// count schedules
			metrics.Schedules.WithLabelValues("created").Inc()
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
			return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
		}

		status := t_api.StatusScheduleAlreadyExists
		if s.IdempotencyKey.Match(req.IdempotencyKey) {
			status = t_api.StatusOK
		}

		res = &t_api.Response{
			Status:   status,
			Metadata: r.Metadata,
			Payload: &t_api.CreateScheduleResponse{
				Schedule: s,
			},
		}
	}

	util.Assert(res != nil, "response must not be nil")
	return res, nil
}

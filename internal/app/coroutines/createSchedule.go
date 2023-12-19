package coroutines

import (
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/metadata"
	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/schedule"
)

type Coroutine = scheduler.Coroutine[*t_aio.Completion, *t_aio.Submission]

type CallBackFn = func(*t_api.Response, error)

func CreateSchedule(metadata *metadata.Metadata, req *t_api.Request, res CallBackFn) *Coroutine {
	return scheduler.NewCoroutine(metadata, func(c *Coroutine) {

		// check if it already exists.

		completion, err := c.Yield(&t_aio.Submission{
			Kind: t_aio.Store,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []*t_aio.Command{
						{
							Kind: t_aio.ReadSchedule,
							ReadSchedule: &t_aio.ReadScheduleCommand{
								Id: req.CreateSchedule.Id,
							},
						},
					},
				},
			},
		})
		if err != nil {
			slog.Error("failed to read schedule", "req", req, "err", err)
			res(nil, t_api.NewResonateError(t_api.ErrAIOStoreFailure, "failed to read schedule", err))
			return
		}

		util.Assert(completion.Store != nil, "completion must not be nil")
		result := completion.Store.Results[0].ReadSchedule
		util.Assert(result.RowsReturned == 0 || result.RowsReturned == 1, "result must return 0 or 1 rows")

		// create if not found.

		if result.RowsReturned == 0 {
			createdOn := c.Time()
			next, err := util.Next(createdOn, req.CreateSchedule.Cron)
			if err != nil {
				slog.Error("failed to calculate next run time", "req", req, "err", err)
				res(nil, t_api.NewResonateError(t_api.ErrAIOStoreSerializationFailure, "failed to calculate next run time", err))
				return
			}

			completion, err := c.Yield(&t_aio.Submission{
				Kind: t_aio.Store,
				Store: &t_aio.StoreSubmission{
					Transaction: &t_aio.Transaction{
						Commands: []*t_aio.Command{
							{
								Kind: t_aio.CreateSchedule,
								CreateSchedule: &t_aio.CreateScheduleCommand{
									Id:             req.CreateSchedule.Id,
									Cron:           req.CreateSchedule.Cron,
									Desc:           req.CreateSchedule.Desc,
									PromiseId:      req.CreateSchedule.PromiseId,
									PromiseParam:   req.CreateSchedule.PromiseParam,
									PromiseTimeout: req.CreateSchedule.PromiseTimeout,
									LastRunTime:    nil,
									NextRunTime:    next,
									CreatedOn:      createdOn,
								},
							},
						},
					},
				},
			})
			if err != nil {
				slog.Error("failed to create schedule", "req", req, "err", err)
				res(nil, t_api.NewResonateError(t_api.ErrAIOStoreFailure, "failed to create schedule", err))
				return
			}

			util.Assert(completion.Store != nil, "completion must not be nil")
			result := completion.Store.Results[0].CreateSchedule
			util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

			if result.RowsAffected == 1 {
				res(
					&t_api.Response{
						Kind: t_api.CreateSchedule,
						CreateSchedule: &t_api.CreateScheduleResponse{
							Status: t_api.StatusCreated,
							Schedule: &schedule.Schedule{
								Id:             req.CreateSchedule.Id,
								Desc:           req.CreateSchedule.Desc,
								Cron:           req.CreateSchedule.Cron,
								PromiseId:      req.CreateSchedule.PromiseId,
								PromiseParam:   req.CreateSchedule.PromiseParam,
								PromiseTimeout: req.CreateSchedule.PromiseTimeout,
								LastRunTime:    nil,
								NextRunTime:    next,
								CreatedOn:      createdOn,
							},
						},
					},
					nil,
				)
			} else {
				c.Scheduler.Add(CreateSchedule(metadata, req, res))
			}
			return
		}

		// return resource conflict.

		s, err := result.Records[0].Schedule()
		if err != nil {
			slog.Error("failed to parse schedule", "req", req, "err", err)
			res(nil, t_api.NewResonateError(t_api.ErrAIOStoreFailure, "failed to parse schedule", err))
			return
		}

		status := t_api.StatusScheduleAlreadyExists

		// if same idempotency key, return ok.

		if s.IdempotencyKey.Match(req.CreateSchedule.IdempotencyKey) {
			status = t_api.StatusOK
		}

		res(
			&t_api.Response{
				Kind: t_api.CreateSchedule,
				CreateSchedule: &t_api.CreateScheduleResponse{
					Status:   status,
					Schedule: s,
				},
			},
			nil,
		)
	})
}

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
		completion, err := _readSchedule(c, req, req.CreateSchedule.Id)
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
			completion, err := _writeSchedule(c, req, createdOn, req.CreateSchedule.Id)
			if err != nil {
				slog.Error("failed to create schedule", "req", req, "err", err)
				res(nil, t_api.NewResonateError(t_api.ErrAIOStoreFailure, "failed to create schedule", err))
				return
			}

			util.Assert(completion.Store != nil, "completion must not be nil")
			result := completion.Store.Results[0].CreateSchedule
			util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

			if result.RowsAffected == 1 {
				next, err := util.Next(createdOn, req.CreateSchedule.Cron)
				if err != nil {
					slog.Error("failed to calculate next run time", "req", req, "err", err)
					res(nil, t_api.NewResonateError(t_api.ErrAIOStoreSerializationFailure, "failed to calculate next run time", err))
					return
				}
				res(
					&t_api.Response{
						Kind: t_api.CreateSchedule,
						CreateSchedule: &t_api.CreateScheduleResponse{
							Status: t_api.StatusCreated,
							Schedule: &schedule.Schedule{
								Id:          req.CreateSchedule.Id,
								Desc:        req.CreateSchedule.Desc,
								Cron:        req.CreateSchedule.Cron,
								LastRunTime: nil,
								NextRunTime: next,
								CreatedOn:   &createdOn,
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

		res(
			&t_api.Response{
				Kind: t_api.CreateSchedule,
				CreateSchedule: &t_api.CreateScheduleResponse{
					Status:   t_api.StatusScheduleAlreadyExists,
					Schedule: s,
				},
			},
			nil,
		)
	})
}

// rework this? ?
// remove request
func _readSchedule(c *Coroutine, req *t_api.Request, id string) (*t_aio.Completion, error) {
	return c.Yield(&t_aio.Submission{
		Kind: t_aio.Store,
		Store: &t_aio.StoreSubmission{
			Transaction: &t_aio.Transaction{
				Commands: []*t_aio.Command{
					{
						Kind: t_aio.ReadSchedule,
						ReadSchedule: &t_aio.ReadScheduleCommand{
							Id: id,
						},
					},
				},
			},
		},
	})
}

// rework this
// make write opertion to command id cinteraval eec. ?? generalizable /
func _writeSchedule(c *Coroutine, req *t_api.Request, createdOn int64, id string) (*t_aio.Completion, error) {
	return c.Yield(&t_aio.Submission{
		Kind: t_aio.Store,
		Store: &t_aio.StoreSubmission{
			Transaction: &t_aio.Transaction{
				Commands: []*t_aio.Command{
					{
						Kind: t_aio.CreateSchedule,
						CreateSchedule: &t_aio.CreateScheduleCommand{
							Id:   id,
							Cron: req.CreateSchedule.Cron,
							Desc: req.CreateSchedule.Desc,
							// LastRunTime:   nil,
							NextRunTime: 123123123, // calculate this.
							CreatedOn:   createdOn,
						},
					},
				},
			},
		},
	})
}

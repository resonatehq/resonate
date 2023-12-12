package coroutines

import (
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/metadata"
	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
)

func ReadSchedule(metadata *metadata.Metadata, req *t_api.Request, res func(*t_api.Response, error)) *scheduler.Coroutine[*t_aio.Completion, *t_aio.Submission] {

	return scheduler.NewCoroutine(metadata, func(c *scheduler.Coroutine[*t_aio.Completion, *t_aio.Submission]) {
		completion, err := _readSchedule(c, req, req.ReadSchedule.Id)
		if err != nil {
			slog.Error("failed to read schedule", "req", req, "err", err)
			res(nil, t_api.NewResonateError(t_api.ErrAIOStoreFailure, "failed to read schedule", err))
			return
		}

		util.Assert(completion.Store != nil, "completion must not be nil")
		result := completion.Store.Results[0].ReadSchedule
		util.Assert(result.RowsReturned == 0 || result.RowsReturned == 1, "result must return 0 or 1 rows")

		// not found

		if result.RowsReturned == 0 {
			res(&t_api.Response{
				Kind: t_api.ReadSchedule,
				ReadSchedule: &t_api.ReadScheduleResponse{
					Status: t_api.StatusScheduleNotFound,
				},
			}, nil)
			return
		}

		// found

		s, err := result.Records[0].Schedule()
		if err != nil {
			slog.Error("failed to parse schedule record", "record", result.Records[0], "err", err)
			res(nil, t_api.NewResonateError(t_api.ErrAIOStoreSerializationFailure, "failed to parse schedule record", err))
			return
		}

		res(&t_api.Response{
			Kind: t_api.ReadSchedule,
			ReadSchedule: &t_api.ReadScheduleResponse{
				Status:   t_api.StatusOK,
				Schedule: s,
			},
		}, nil)

	})
}

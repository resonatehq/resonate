package coroutines

import (
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
)

func ReadSchedule(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
		Kind: t_aio.Store,
		Tags: r.Tags,
		Store: &t_aio.StoreSubmission{
			Transaction: &t_aio.Transaction{
				Commands: []*t_aio.Command{
					{
						Kind: t_aio.ReadSchedule,
						ReadSchedule: &t_aio.ReadScheduleCommand{
							Id: r.ReadSchedule.Id,
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
		res = &t_api.Response{
			Kind: t_api.ReadSchedule,
			Tags: r.Tags,
			ReadSchedule: &t_api.ReadScheduleResponse{
				Status: t_api.StatusScheduleNotFound,
			},
		}
	} else {
		s, err := result.Records[0].Schedule()
		if err != nil {
			slog.Error("failed to parse schedule record", "record", result.Records[0], "err", err)
			return nil, t_api.NewResonateError(t_api.ErrAIOStoreSerializationFailure, "failed to parse schedule record", err)
		}

		res = &t_api.Response{
			Kind: t_api.ReadSchedule,
			Tags: r.Tags,
			ReadSchedule: &t_api.ReadScheduleResponse{
				Status:   t_api.StatusOK,
				Schedule: s,
			},
		}
	}

	util.Assert(res != nil, "response must not be nil")
	return res, nil
}

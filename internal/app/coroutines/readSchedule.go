package coroutines

import (
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
)

func ReadSchedule(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	req := r.Payload.(*t_api.ReadScheduleRequest)
	completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
		Kind: t_aio.Store,
		Tags: r.Metadata,
		Store: &t_aio.StoreSubmission{
			Transaction: &t_aio.Transaction{
				Commands: []*t_aio.Command{
					{
						Kind: t_aio.ReadSchedule,
						ReadSchedule: &t_aio.ReadScheduleCommand{
							Id: req.Id,
						},
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
	result := completion.Store.Results[0].ReadSchedule
	util.Assert(result.RowsReturned == 0 || result.RowsReturned == 1, "result must return 0 or 1 rows")

	var res *t_api.Response

	if result.RowsReturned == 0 {
		res = &t_api.Response{
			Status:   t_api.StatusScheduleNotFound,
			Metadata: r.Metadata,
			Payload:  &t_api.ReadScheduleResponse{},
		}
	} else {
		s, err := result.Records[0].Schedule()
		if err != nil {
			slog.Error("failed to parse schedule record", "record", result.Records[0], "err", err)
			return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
		}

		res = &t_api.Response{
			Status:   t_api.StatusOK,
			Metadata: r.Metadata,
			Payload: &t_api.ReadScheduleResponse{
				Schedule: s,
			},
		}
	}

	util.Assert(res != nil, "response must not be nil")
	return res, nil
}

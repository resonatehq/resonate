package coroutines

import (
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/resonatehq/resonate/internal/util"
)

func DeleteSchedule(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	req := r.Payload.(*t_api.DeleteScheduleRequest)
	metrics := c.Get("metrics").(*metrics.Metrics)

	completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
		Kind: t_aio.Store,
		Tags: r.Metadata,
		Store: &t_aio.StoreSubmission{
			Transaction: &t_aio.Transaction{
				Commands: []t_aio.Command{
					&t_aio.DeleteScheduleCommand{
						Id: req.Id,
					},
				},
			},
		},
	})
	if err != nil {
		slog.Error("failed to delete schedule", "req", r, "err", err)
		return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
	}

	util.Assert(completion.Store != nil, "completion must not be nil")
	result := t_aio.AsAlterSchedules(completion.Store.Results[0])
	util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

	var status t_api.StatusCode
	if result.RowsAffected == 1 {
		status = t_api.StatusNoContent

		// count schedules
		metrics.SchedulesTotal.WithLabelValues("deleted").Inc()
	} else {
		status = t_api.StatusScheduleNotFound
	}

	return &t_api.Response{
		Status:   status,
		Metadata: r.Metadata,
		Payload:  &t_api.DeleteScheduleResponse{},
	}, nil
}

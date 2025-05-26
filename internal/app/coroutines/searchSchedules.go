package coroutines

import (
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/schedule"
)

func SearchSchedules(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	req := r.Payload.(*t_api.SearchSchedulesRequest)
	util.Assert(req.Id != "", "id must not be empty")
	util.Assert(req.Limit > 0, "limit must be greater than zero")

	if req.Tags == nil {
		req.Tags = map[string]string{}
	}

	completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
		Kind: t_aio.Store,
		Tags: r.Metadata,
		Store: &t_aio.StoreSubmission{
			Transaction: &t_aio.Transaction{
				Commands: []t_aio.Command{
					&t_aio.SearchSchedulesCommand{
						Id:     req.Id,
						Tags:   req.Tags,
						Limit:  req.Limit,
						SortId: req.SortId,
					},
				},
			},
		},
	})

	if err != nil {
		slog.Error("failed to search schedules", "req", r, "err", err)
		return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
	}

	util.Assert(completion.Store != nil, "completion must not be nil")

	result := t_aio.AsQuerySchedules(completion.Store.Results[0])
	schedules := []*schedule.Schedule{}

	for _, record := range result.Records {
		schedule, err := record.Schedule()
		if err != nil {
			slog.Warn("failed to parse schedule record", "record", record, "err", err)
			continue
		}

		schedules = append(schedules, schedule)
	}

	// set cursor only if there are more results
	var cursor *t_api.Cursor[t_api.SearchSchedulesRequest]
	if result.RowsReturned == int64(req.Limit) {
		cursor = &t_api.Cursor[t_api.SearchSchedulesRequest]{
			Next: &t_api.SearchSchedulesRequest{
				Id:     req.Id,
				Tags:   req.Tags,
				Limit:  req.Limit,
				SortId: &result.LastSortId,
			},
		}
	}

	return &t_api.Response{
		Status:   t_api.StatusOK,
		Metadata: r.Metadata,
		Payload: &t_api.SearchSchedulesResponse{
			Cursor:    cursor,
			Schedules: schedules,
		},
	}, nil
}

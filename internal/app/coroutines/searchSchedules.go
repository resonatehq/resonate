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

func SearchSchedules(metadata *metadata.Metadata, req *t_api.Request, res func(*t_api.Response, error)) *scheduler.Coroutine[*t_aio.Completion, *t_aio.Submission] {
	return scheduler.NewCoroutine(metadata, func(c *scheduler.Coroutine[*t_aio.Completion, *t_aio.Submission]) {
		util.Assert(req.SearchSchedules.Id != "", "id must not be empty")
		util.Assert(req.SearchSchedules.Limit > 0, "limit must be greater than zero")

		if req.SearchSchedules.Tags == nil {
			req.SearchSchedules.Tags = map[string]string{}
		}

		completion, err := c.Yield(&t_aio.Submission{
			Kind: t_aio.Store,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []*t_aio.Command{
						{
							Kind: t_aio.SearchSchedules,
							SearchSchedules: &t_aio.SearchSchedulesCommand{
								Id:     req.SearchSchedules.Id,
								Tags:   req.SearchSchedules.Tags,
								Limit:  req.SearchSchedules.Limit,
								SortId: req.SearchSchedules.SortId,
							},
						},
					},
				},
			},
		})

		if err != nil {
			slog.Error("failed to search schedules", "req", req, "err", err)
			res(nil, t_api.NewResonateError(t_api.ErrAIOStoreFailure, "failed to search promises", err))
			return
		}

		util.Assert(completion.Store != nil, "completion must not be nil")

		result := completion.Store.Results[0].SearchSchedules
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
		if result.RowsReturned == int64(req.SearchSchedules.Limit) {
			cursor = &t_api.Cursor[t_api.SearchSchedulesRequest]{
				Next: &t_api.SearchSchedulesRequest{
					Id:     req.SearchSchedules.Id,
					Tags:   req.SearchSchedules.Tags,
					Limit:  req.SearchSchedules.Limit,
					SortId: &result.LastSortId,
				},
			}
		}

		res(&t_api.Response{
			Kind: t_api.SearchSchedules,
			SearchSchedules: &t_api.SearchSchedulesResponse{
				Status:    t_api.StatusOK,
				Cursor:    cursor,
				Schedules: schedules,
			},
		}, nil)
	})
}

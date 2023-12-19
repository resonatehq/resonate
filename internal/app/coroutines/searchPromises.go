package coroutines

import (
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/metadata"
	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
)

func SearchPromises(metadata *metadata.Metadata, req *t_api.Request, res func(*t_api.Response, error)) *scheduler.Coroutine[*t_aio.Completion, *t_aio.Submission] {
	return scheduler.NewCoroutine(metadata, func(c *scheduler.Coroutine[*t_aio.Completion, *t_aio.Submission]) {
		util.Assert(req.SearchPromises.Id != "", "id must not be empty")
		util.Assert(req.SearchPromises.Limit > 0, "limit must be greater than zero")

		if req.SearchPromises.Tags == nil {
			req.SearchPromises.Tags = map[string]string{}
		}

		completion, err := c.Yield(&t_aio.Submission{
			Kind: t_aio.Store,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []*t_aio.Command{
						{
							Kind: t_aio.TimeoutCreateNotifications,
							TimeoutCreateNotifications: &t_aio.TimeoutCreateNotificationsCommand{
								Time: c.Time(),
							},
						},
						{
							Kind: t_aio.TimeoutDeleteSubscriptions,
							TimeoutDeleteSubscriptions: &t_aio.TimeoutDeleteSubscriptionsCommand{
								Time: c.Time(),
							},
						},
						{
							Kind: t_aio.TimeoutPromises,
							TimeoutPromises: &t_aio.TimeoutPromisesCommand{
								Time: c.Time(),
							},
						},
						{
							Kind: t_aio.SearchPromises,
							SearchPromises: &t_aio.SearchPromisesCommand{
								Id:     req.SearchPromises.Id,
								States: req.SearchPromises.States,
								Tags:   req.SearchPromises.Tags,
								Limit:  req.SearchPromises.Limit,
								SortId: req.SearchPromises.SortId,
							},
						},
					},
				},
			},
		})

		if err != nil {
			slog.Error("failed to search promises", "req", req, "err", err)
			res(nil, t_api.NewResonateError(t_api.ErrAIOStoreFailure, "failed to search promises", err))
			return
		}

		util.Assert(completion.Store != nil, "completion must not be nil")
		util.Assert(len(completion.Store.Results) == 4, "must have four results")

		result := completion.Store.Results[3].SearchPromises
		promises := []*promise.Promise{}

		for _, record := range result.Records {
			promise, err := record.Promise()
			if err != nil {
				slog.Warn("failed to parse promise record", "record", record, "err", err)
				continue
			}

			promises = append(promises, promise)
		}

		// set cursor only if there are more results
		var cursor *t_api.Cursor[t_api.SearchPromisesRequest]
		if result.RowsReturned == int64(req.SearchPromises.Limit) {
			cursor = &t_api.Cursor[t_api.SearchPromisesRequest]{
				Next: &t_api.SearchPromisesRequest{
					Id:     req.SearchPromises.Id,
					States: req.SearchPromises.States,
					Tags:   req.SearchPromises.Tags,
					Limit:  req.SearchPromises.Limit,
					SortId: &result.LastSortId,
				},
			}
		}

		res(&t_api.Response{
			Kind: t_api.SearchPromises,
			SearchPromises: &t_api.SearchPromisesResponse{
				Status:   t_api.StatusOK,
				Cursor:   cursor,
				Promises: promises,
			},
		}, nil)
	})
}

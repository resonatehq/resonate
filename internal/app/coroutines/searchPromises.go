package coroutines

import (
	"fmt"
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
)

func SearchPromises(t int64, req *types.Request, res func(int64, *types.Response, error)) *scheduler.Coroutine {
	return scheduler.NewCoroutine(fmt.Sprintf("SearchPromises(q=%s)", req.SearchPromises.Q), "SearchPromises", func(s *scheduler.Scheduler, c *scheduler.Coroutine) {
		submission := &types.Submission{
			Kind: types.Store,
			Store: &types.StoreSubmission{
				Transaction: &types.Transaction{
					Commands: []*types.Command{
						{
							Kind: types.StoreTimeoutCreateNotifications,
							TimeoutCreateNotifications: &types.TimeoutCreateNotificationsCommand{
								Time: t,
							},
						},
						{
							Kind: types.StoreTimeoutDeleteSubscriptions,
							TimeoutDeleteSubscriptions: &types.TimeoutDeleteSubscriptionsCommand{
								Time: t,
							},
						},
						{
							Kind: types.StoreTimeoutPromises,
							TimeoutPromises: &types.TimeoutPromisesCommand{
								Time: t,
							},
						},
						{
							Kind: types.StoreSearchPromises,
							SearchPromises: &types.SearchPromisesCommand{
								Q:      req.SearchPromises.Q,
								States: req.SearchPromises.States,
								Limit:  req.SearchPromises.Limit,
								SortId: req.SearchPromises.SortId,
							},
						},
					},
				},
			},
		}

		c.Yield(submission, func(t int64, completion *types.Completion, err error) {
			if err != nil {
				slog.Error("failed to search promises", "req", req, "err", err)
				res(t, nil, err)
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
			var cursor *types.Cursor[types.SearchPromisesRequest]
			if result.RowsReturned == int64(req.SearchPromises.Limit) {
				cursor = &types.Cursor[types.SearchPromisesRequest]{
					Next: &types.SearchPromisesRequest{
						Q:      req.SearchPromises.Q,
						States: req.SearchPromises.States,
						Limit:  req.SearchPromises.Limit,
						SortId: &result.LastSortId,
					},
				}
			}

			res(t, &types.Response{
				Kind: types.SearchPromises,
				SearchPromises: &types.SearchPromisesResponse{
					Status:   types.ResponseOK,
					Cursor:   cursor,
					Promises: promises,
				},
			}, nil)
		})
	})
}

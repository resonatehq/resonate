package coroutines

import (
	"fmt"
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
)

func SearchPromises(t1 int64, req *types.Request, res func(int64, *types.Response, error)) *scheduler.Coroutine {
	return scheduler.NewCoroutine(fmt.Sprintf("SearchPromises(q=%s)", req.SearchPromises.Q), "SearchPromises", func(s *scheduler.Scheduler, c *scheduler.Coroutine) {
		submission := &types.Submission{
			Kind: types.Store,
			Store: &types.StoreSubmission{
				Transaction: &types.Transaction{
					Commands: []*types.Command{
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

		c.Yield(submission, func(t2 int64, completion *types.Completion, err error) {
			if err != nil {
				slog.Error("failed to search promises", "req", req, "err", err)
				res(t2, nil, err)
				return
			}

			util.Assert(completion.Store != nil, "completion must not be nil")

			result := completion.Store.Results[0].SearchPromises
			promises := []*promise.Promise{}

			var timedoutInSearch bool
			for _, state := range req.SearchPromises.States {
				if state == promise.Timedout {
					timedoutInSearch = true
					break
				}
			}

			for _, record := range result.Records {
				p, err := record.Promise()
				if err != nil {
					slog.Warn("failed to parse promise record", "record", record, "err", err)
					continue
				}

				if p.State == promise.Pending && t1 >= p.Timeout {
					// ignore "timedout" promise if not in search
					if !timedoutInSearch {
						continue
					}

					// set to "timedout" if (request) time is greater than timeout
					p.State = promise.Timedout
					p.Value = promise.Value{
						Headers: map[string]string{},
						Ikey:    nil,
						Data:    nil,
					}
					p.CompletedOn = &p.Timeout
				}

				promises = append(promises, p)
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

			res(t2, &types.Response{
				Kind: types.SearchPromises,
				SearchPromises: &types.SearchPromisesResponse{
					Status:   types.ResponseOK,
					Promises: promises,
					Cursor:   cursor,
				},
			}, nil)
		})
	})
}

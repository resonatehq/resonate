package coroutines

import (
	"fmt"
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
)

func SearchPromises(t int64, req *types.Request, res func(*types.Response, error)) *scheduler.Coroutine {
	return scheduler.NewCoroutine(fmt.Sprintf("SearchPromises(q=%s)", req.SearchPromises.Q), "SearchPromises", func(s *scheduler.Scheduler, c *scheduler.Coroutine) {
		submission := &types.Submission{
			Kind: types.Store,
			Store: &types.StoreSubmission{
				Transaction: &types.Transaction{
					Commands: []*types.Command{
						{
							Kind: types.StoreSearchPromises,
							SearchPromises: &types.SearchPromisesCommand{
								Q:     req.SearchPromises.Q,
								State: req.SearchPromises.State,
							},
						},
					},
				},
			},
		}

		c.Yield(submission, func(completion *types.Completion, err error) {
			if err != nil {
				slog.Error("failed to search promises", "req", req, "err", err)
				res(nil, err)
				return
			}

			util.Assert(completion.Store != nil, "completion must not be nil")

			result := completion.Store.Results[0].SearchPromises
			promises := []*promise.Promise{}

			for _, record := range result.Records {
				if t < record.Timeout {
					promise, err := record.Promise()
					if err != nil {
						slog.Warn("failed to parse promise record", "record", record, "err", err)
						continue
					}

					promises = append(promises, promise)
				}
			}

			res(&types.Response{
				Kind: types.SearchPromises,
				SearchPromises: &types.SearchPromisesResponse{
					Status:   types.ResponseOK,
					Promises: promises,
				},
			}, nil)
		})
	})
}

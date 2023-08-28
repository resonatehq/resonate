package coroutines

import (
	"fmt"
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
)

func ReadPromise(t int64, req *types.Request, res func(*types.Response, error)) *scheduler.Coroutine {
	return scheduler.NewCoroutine(fmt.Sprintf("ReadPromise(id=%s)", req.ReadPromise.Id), func(s *scheduler.Scheduler, c *scheduler.Coroutine) {
		submission := &types.Submission{
			Kind: types.Store,
			Store: &types.StoreSubmission{
				Transaction: &types.Transaction{
					Commands: []*types.Command{
						{
							Kind: types.StoreReadPromise,
							ReadPromise: &types.ReadPromiseCommand{
								Id: req.ReadPromise.Id,
							},
						},
					},
				},
			},
		}

		c.Yield(submission, func(completion *types.Completion, err error) {
			if err != nil {
				slog.Error("failed to read promise", "req", req, "err", err)
				res(nil, err)
				return
			}

			util.Assert(completion.Store != nil, "completion must not be nil")

			result := completion.Store.Results[0].ReadPromise
			util.Assert(result.RowsReturned == 0 || result.RowsReturned == 1, "result must return 0 or 1 rows")

			if result.RowsReturned == 0 {
				res(&types.Response{
					Kind: types.ReadPromise,
					ReadPromise: &types.ReadPromiseResponse{
						Status: types.ResponseNotFound,
					},
				}, nil)
			} else {
				record := result.Records[0]

				if record.State == promise.Pending && t >= record.Timeout {
					s.Add(TimeoutPromise(t, req.ReadPromise.Id, ReadPromise(t, req, res), func(err error) {
						if err != nil {
							slog.Error("failed to timeout promise", "req", req, "err", err)
							res(nil, err)
							return
						}

						param, err := record.Param()
						if err != nil {
							slog.Error("failed to parse promise record param", "record", record, "err", err)
							res(nil, err)
							return
						}

						res(&types.Response{
							Kind: types.ReadPromise,
							ReadPromise: &types.ReadPromiseResponse{
								Status: types.ResponseOK,
								Promise: &promise.Promise{
									Id:    record.Id,
									State: promise.Timedout,
									Param: param,
									Value: promise.Value{
										Headers: map[string]string{},
										Ikey:    nil,
										Data:    nil,
									},
									Timeout: record.Timeout,
								},
							},
						}, nil)
					}))
				} else {
					p, err := record.Promise()
					if err != nil {
						slog.Error("failed to parse promise record", "record", record, "err", err)
						res(nil, err)
						return
					}

					res(&types.Response{
						Kind: types.ReadPromise,
						ReadPromise: &types.ReadPromiseResponse{
							Status:  types.ResponseOK,
							Promise: p,
						},
					}, nil)
				}
			}
		})
	})
}

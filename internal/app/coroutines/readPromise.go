package coroutines

import (
	"fmt"
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
)

func ReadPromise(t int64, req *types.Request, res func(int64, *types.Response, error)) *scheduler.Coroutine {
	return scheduler.NewCoroutine(fmt.Sprintf("ReadPromise(id=%s)", req.ReadPromise.Id), "ReadPromise", func(s *scheduler.Scheduler, c *scheduler.Coroutine) {
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

		c.Yield(submission, func(t int64, completion *types.Completion, err error) {
			if err != nil {
				slog.Error("failed to read promise", "req", req, "err", err)
				res(t, nil, err)
				return
			}

			util.Assert(completion.Store != nil, "completion must not be nil")

			result := completion.Store.Results[0].ReadPromise
			util.Assert(result.RowsReturned == 0 || result.RowsReturned == 1, "result must return 0 or 1 rows")

			if result.RowsReturned == 0 {
				res(t, &types.Response{
					Kind: types.ReadPromise,
					ReadPromise: &types.ReadPromiseResponse{
						Status: types.ResponseNotFound,
					},
				}, nil)
			} else {
				p, err := result.Records[0].Promise()
				if err != nil {
					slog.Error("failed to parse promise record", "record", result.Records[0], "err", err)
					res(t, nil, err)
					return
				}

				if p.State == promise.Pending && t >= p.Timeout {
					s.Add(TimeoutPromise(t, p, ReadPromise(t, req, res), func(t int64, err error) {
						if err != nil {
							slog.Error("failed to timeout promise", "req", req, "err", err)
							res(t, nil, err)
							return
						}

						res(t, &types.Response{
							Kind: types.ReadPromise,
							ReadPromise: &types.ReadPromiseResponse{
								Status: types.ResponseOK,
								Promise: &promise.Promise{
									Id:    p.Id,
									State: promise.Timedout,
									Param: p.Param,
									Value: promise.Value{
										Headers: map[string]string{},
										Ikey:    nil,
										Data:    nil,
									},
									Timeout:     p.Timeout,
									Tags:        p.Tags,
									CreatedOn:   p.CreatedOn,
									CompletedOn: &p.Timeout,
								},
							},
						}, nil)
					}))
				} else {
					res(t, &types.Response{
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

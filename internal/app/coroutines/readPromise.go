package coroutines

import (
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
)

func ReadPromise(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	req := r.Data.(*t_api.PromiseGetRequest)

	completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
		Kind: t_aio.Store,
		Tags: r.Head,
		Store: &t_aio.StoreSubmission{
			Transaction: &t_aio.Transaction{
				Commands: []t_aio.Command{
					&t_aio.ReadPromiseCommand{
						Id: req.Id,
					},
				},
			},
		},
	})

	if err != nil {
		slog.Error("failed to read promise", "req", r, "err", err)
		return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
	}

	util.Assert(completion.Store != nil, "completion must not be nil")

	result := t_aio.AsQueryPromises(completion.Store.Results[0])
	util.Assert(result.RowsReturned == 0 || result.RowsReturned == 1, "result must return 0 or 1 rows")

	var res *t_api.Response

	if result.RowsReturned == 1 {
		p, err := result.Records[0].Promise()
		if err != nil {
			slog.Error("failed to parse promise record", "record", result.Records[0], "err", err)
			return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
		}

		if p.State == promise.Pending && p.Timeout <= c.Time() {
			cmd := &t_aio.UpdatePromiseCommand{
				Id:          req.Id,
				State:       promise.GetTimedoutState(p.Tags),
				Value:       promise.Value{},
				CompletedOn: p.Timeout,
			}

			ok, err := gocoro.SpawnAndAwait(c, completePromise(r.Head, cmd))
			if err != nil {
				return nil, err
			}

			if !ok {
				// It's possible that the promise was completed by another coroutine
				// while we were timing out. In that case, we should just retry.
				return ReadPromise(c, r)
			}

			res = &t_api.Response{
				Status:   t_api.StatusOK,
				Head: r.Head,
				Data: &t_api.PromiseGetResponse{
					Promise: &promise.Promise{
						Id:          p.Id,
						State:       cmd.State,
						Param:       p.Param,
						Value:       cmd.Value,
						Timeout:     p.Timeout,
						Tags:        p.Tags,
						CreatedOn:   p.CreatedOn,
						CompletedOn: &cmd.CompletedOn,
					},
				},
			}
		} else {
			res = &t_api.Response{
				Status:   t_api.StatusOK,
				Head: r.Head,
				Data: &t_api.PromiseGetResponse{
					Promise: p,
				},
			}
		}
	} else {
		res = &t_api.Response{
			Status:   t_api.StatusPromiseNotFound,
			Head: r.Head,
			Data:  &t_api.PromiseGetResponse{},
		}
	}

	util.Assert(res != nil, "response must not be nil")
	return res, nil
}

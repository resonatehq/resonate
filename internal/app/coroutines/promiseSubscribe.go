package coroutines

import (
	"errors"
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/message"
	"github.com/resonatehq/resonate/pkg/promise"
)

func PromiseSubscribe(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	req := r.Data.(*t_api.PromiseSubscribeRequest)
	util.Assert(req != nil, "request must not be nil")

	awaited, err := gocoro.SpawnAndAwait(c, readPromise(r.Head, req.Awaited))
	if err != nil {
		return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
	}

	if awaited == nil {
		return &t_api.Response{
			Status: t_api.StatusPromiseNotFound,
			Head:   r.Head,
			Data:   &t_api.PromiseSubscribeResponse{},
		}, nil
	}

	if awaited.State == promise.Pending {
		createdOn := c.Time()

		completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
			Kind: t_aio.Store,
			Tags: r.Head,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []t_aio.Command{
						&t_aio.CreateCallbackCommand{
							Id:        util.NotifyId(awaited.Id, req.Address),
							PromiseId: awaited.Id,
							Recv:      []byte(req.Address),
							Mesg: &message.Mesg{
								Type: "notify",
								Head: r.Head,
								Root: awaited.Id,
							},
							Timeout:   awaited.Timeout,
							CreatedOn: createdOn,
						},
					},
				},
			},
		})

		if err != nil {
			slog.Error("failed to create callback", "req", r, "err", err)
			return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
		}

		if !completion.Store.Valid {
			return nil, t_api.NewError(t_api.StatusTaskPreconditionFailed, errors.New("the specified task is not valid"))
		}

		util.Assert(completion.Store != nil, "completion must not be nil")
		util.Assert(len(completion.Store.Results) == 1, "completion must have one result")

		result := t_aio.AsAlterCallbacks(completion.Store.Results[0])
		util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

		if result.RowsAffected == 1 {
			slog.Debug("subscription created", "id", util.NotifyId(awaited.Id, req.Address))
		} else {
			slog.Debug("subscription deduped", "id", util.NotifyId(awaited.Id, req.Address))
		}
	}

	return &t_api.Response{
		Status: t_api.StatusOK,
		Head:   r.Head,
		Data: &t_api.PromiseSubscribeResponse{
			Promise: awaited,
		},
	}, nil
}

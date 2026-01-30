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

func PromiseRegister(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	req := r.Data.(*t_api.PromiseRegisterRequest)
	util.Assert(req != nil, "request must not be nil")

	awaiterF := gocoro.Spawn(c, readPromise(r.Head, req.Awaiter))
	awaitedF := gocoro.Spawn(c, readPromise(r.Head, req.Awaited))

	awaiter, err := gocoro.Await(c, awaiterF)
	if err != nil {
		return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
	}
	awaited, err := gocoro.Await(c, awaitedF)
	if err != nil {
		return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
	}

	if awaiter == nil || awaited == nil {
		return &t_api.Response{
			Status: t_api.StatusPromiseNotFound,
			Head:   r.Head,
			Data:   &t_api.PromiseRegisterResponse{},
		}, nil
	}

	recv, exists := awaiter.Tags["resonate:invoke"]
	if !exists {
		slog.Debug("Awaiter promise missing recv tag", "tags", awaiter.Tags)
		return &t_api.Response{
			Status: t_api.StatusPromiseRecvNotFound,
			Head:   r.Head,
			Data:   &t_api.PromiseRegisterResponse{},
		}, nil
	}

	if awaited.State == promise.Pending && awaiter.State == promise.Pending {
		createdOn := c.Time()

		completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
			Kind: t_aio.Store,
			Tags: r.Head,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []t_aio.Command{
						&t_aio.CreateCallbackCommand{
							Id:        util.ResumeId(awaiter.Id, awaited.Id),
							PromiseId: awaited.Id,
							Recv:      []byte(recv),
							Mesg: &message.Mesg{
								Type: "resume",
								Head: map[string]string{},
								Root: awaiter.Id,
								Leaf: awaited.Id,
							},
							Timeout:   awaiter.Timeout,
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
			slog.Debug("callback created", "id", util.ResumeId(awaiter.Id, awaited.Id))
		} else {
			slog.Debug("callback deduped", "id", util.ResumeId(awaiter.Id, awaited.Id))
		}
	}

	return &t_api.Response{
		Status: t_api.StatusOK,
		Head:   r.Head,
		Data: &t_api.PromiseRegisterResponse{
			Promise: awaited,
		},
	}, nil
}

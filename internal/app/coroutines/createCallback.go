package coroutines

import (
	"errors"
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/callback"
	"github.com/resonatehq/resonate/pkg/promise"
)

func CreateCallback(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	req := r.Data.(*t_api.PromiseRegisterRequest)
	util.Assert(req != nil, "create callback must not be nil")
	util.Assert(req.Mesg.Type == "resume" || req.Mesg.Type == "notify", "message type must be resume or notify")
	util.Assert(req.Mesg.Type == "resume" || req.PromiseId == req.Mesg.Root, "if notify, root promise id must equal leaf promise id")
	util.Assert(req.Mesg.Type == "notify" || (req.PromiseId == req.Mesg.Leaf && req.PromiseId != req.Mesg.Root), "if resume, root promise id must not equal leaf promise id")

	var res *t_api.Response

	// read the promise to see if it exists
	completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
		Kind: t_aio.Store,
		Tags: r.Head,
		Store: &t_aio.StoreSubmission{
			Transaction: &t_aio.Transaction{
				Commands: []t_aio.Command{
					&t_aio.ReadPromiseCommand{
						Id: req.PromiseId,
					},
				},
			},
		},
	})

	if err != nil {
		slog.Error("failed to read promise", "req", r, "err", err)
		return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
	}

	if !completion.Store.Valid {
		return nil, t_api.NewError(t_api.StatusTaskPreconditionFailed, errors.New("the specified task is not valid"))
	}

	util.Assert(completion.Store != nil, "completion must not be nil")
	util.Assert(len(completion.Store.Results) == 1, "completion must have one result")

	result := t_aio.AsQueryPromises(completion.Store.Results[0])
	util.Assert(result.RowsReturned == 0 || result.RowsReturned == 1, "result must return 0 or 1 rows")

	if result.RowsReturned == 1 {
		p, err := result.Records[0].Promise()
		if err != nil {
			slog.Error("failed to parse promise record", "record", result.Records[0], "err", err)
			return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
		}

		// If the callback was already created return 200 and an empty callback
		var cb *callback.Callback
		status := t_api.StatusOK

		if p.State == promise.Pending {
			createdOn := c.Time()

			completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
				Kind: t_aio.Store,
				Tags: r.Head,
				Store: &t_aio.StoreSubmission{
					Transaction: &t_aio.Transaction{
						Commands: []t_aio.Command{
							&t_aio.CreateCallbackCommand{
								Id:        req.Id,
								PromiseId: req.PromiseId,
								Recv:      req.Recv,
								Mesg:      req.Mesg,
								Timeout:   req.Timeout,
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
				status = t_api.StatusCreated
				cb = &callback.Callback{
					Id:        req.Id,
					PromiseId: req.PromiseId,
					Recv:      req.Recv,
					Mesg:      req.Mesg,
					Timeout:   req.Timeout,
					CreatedOn: createdOn,
				}
			}
		}

		res = &t_api.Response{
			// Status could be StatusOk or StatusCreated if the Callback Id was already present
			Status:   status,
			Head: r.Head,
			Data: &t_api.PromiseRegisterResponse{
				Callback: cb,
				Promise:  p,
			},
		}
	} else {
		res = &t_api.Response{
			Status:   t_api.StatusPromiseNotFound,
			Head: r.Head,
			Data:  &t_api.PromiseRegisterResponse{},
		}
	}

	util.Assert(res != nil, "response must not be nil")
	return res, nil
}

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

	promise, err := gocoro.SpawnAndAwait(c, readPromise(r.Head, req.Id))
	if err != nil {
		slog.Error("failed to read promise", "id", req.Id, "err", err)
		return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
	}

	if promise == nil {
		return &t_api.Response{
			Status: t_api.StatusPromiseNotFound,
			Head:   r.Head,
			Data:   &t_api.PromiseGetResponse{},
		}, nil
	}

	return &t_api.Response{
		Status: t_api.StatusOK,
		Head:   r.Head,
		Data: &t_api.PromiseGetResponse{
			Promise: promise,
		},
	}, nil
}

func readPromise(head map[string]string, id string) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, *promise.Promise] {
	return func(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, *promise.Promise]) (*promise.Promise, error) {
		completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
			Kind: t_aio.Store,
			Tags: head,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []t_aio.Command{
						&t_aio.ReadPromiseCommand{
							Id: id,
						},
					},
				},
			},
		})

		if err != nil {
			slog.Error("failed to read promise", "id", id, "err", err)
			return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
		}

		util.Assert(completion.Store != nil, "completion must not be nil")

		result := t_aio.AsQueryPromises(completion.Store.Results[0])
		util.Assert(result.RowsReturned == 0 || result.RowsReturned == 1, "result must return 0 or 1 rows")

		if result.RowsReturned == 0 {
			return nil, nil
		}

		p, err := result.Records[0].Promise()
		if err != nil {
			slog.Error("failed to parse promise record", "record", result.Records[0], "err", err)
			return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
		}

		if p.State == promise.Pending && p.Timeout <= c.Time() {
			cmd := &t_aio.UpdatePromiseCommand{
				Id:          p.Id,
				State:       promise.GetTimedoutState(p.Tags),
				Value:       promise.Value{},
				CompletedOn: p.Timeout,
			}

			ok, err := gocoro.SpawnAndAwait(c, completePromise(head, cmd))
			if err != nil {
				return nil, err
			}

			if !ok {
				// It's possible that the promise was completed by another coroutine
				// while we were timing out. In that case, we should just retry.
				return gocoro.SpawnAndAwait(c, readPromise(head, id))
			}

			// Update the promise before returning it
			p.State = promise.GetTimedoutState(p.Tags)
			p.Value = promise.Value{}
			p.CompletedOn = util.ToPointer(p.Timeout)
		}

		return p, nil
	}
}

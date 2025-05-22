package coroutines

import (
	"log/slog"

	"github.com/resonatehq/gocoro"
	gocoroPromise "github.com/resonatehq/gocoro/pkg/promise"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
)

func SearchPromises(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	req := r.Payload.(*t_api.SearchPromisesRequest)

	util.Assert(req.Id != "", "id must not be empty")
	util.Assert(req.Limit > 0, "limit must be greater than zero")

	if req.Tags == nil {
		req.Tags = map[string]string{}
	}

	completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
		Kind: t_aio.Store,
		Tags: r.Metadata,
		Store: &t_aio.StoreSubmission{
			Transaction: &t_aio.Transaction{
				Commands: []*t_aio.Command{
					{
						Kind: t_aio.SearchPromises,
						SearchPromises: &t_aio.SearchPromisesCommand{
							Id:     req.Id,
							States: req.States,
							Tags:   req.Tags,
							Limit:  req.Limit,
							SortId: req.SortId,
						},
					},
				},
			},
		},
	})

	if err != nil {
		slog.Error("failed to search promises", "req", r, "err", err)
		return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
	}

	util.Assert(completion.Store != nil, "completion must not be nil")
	util.Assert(len(completion.Store.Results) == 1, "must have two results")

	result := completion.Store.Results[0].SearchPromises
	promises := []*promise.Promise{}
	awaiting := []gocoroPromise.Awaitable[bool]{}

	for _, record := range result.Records {
		p, err := record.Promise()
		if err != nil {
			slog.Warn("failed to parse promise record", "record", record, "err", err)
			continue
		}

		promises = append(promises, p)

		if p.State == promise.Pending && p.Timeout <= c.Time() {
			awaiting = append(awaiting, gocoro.Spawn(c, completePromise(r.Metadata, &t_aio.UpdatePromiseCommand{
				Id:             p.Id,
				State:          promise.GetTimedoutState(p),
				Value:          promise.Value{},
				IdempotencyKey: nil,
				CompletedOn:    p.Timeout,
			})))
		}
	}

	for _, p := range awaiting {
		if _, err := gocoro.Await(c, p); err != nil {
			return nil, err
		}
	}

	if len(awaiting) > 0 {
		// If we lazily timeout promises we need to search again
		return SearchPromises(c, r)
	}

	// set cursor only if there are more results
	var cursor *t_api.Cursor[t_api.SearchPromisesRequest]
	if result.RowsReturned == int64(req.Limit) {
		cursor = &t_api.Cursor[t_api.SearchPromisesRequest]{
			Next: &t_api.SearchPromisesRequest{
				Id:     req.Id,
				States: req.States,
				Tags:   req.Tags,
				Limit:  req.Limit,
				SortId: &result.LastSortId,
			},
		}
	}

	return &t_api.Response{
		Status:   t_api.StatusOK,
		Metadata: r.Metadata,
		Payload: &t_api.SearchPromisesResponse{
			Cursor:   cursor,
			Promises: promises,
		},
	}, nil
}

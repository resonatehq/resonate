package store

import (
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/util"
)

type Store interface {
	Execute([]*t_aio.Transaction) ([][]*t_aio.Result, error)
}

func Process(store Store, sqes []*bus.SQE[t_aio.Submission, t_aio.Completion]) []*bus.CQE[t_aio.Submission, t_aio.Completion] {
	var cqes []*bus.CQE[t_aio.Submission, t_aio.Completion]
	var transactions []*t_aio.Transaction

	for _, sqe := range sqes {
		util.Assert(sqe.Submission.Store != nil, "submission must not be nil")
		transactions = append(transactions, sqe.Submission.Store.Transaction)
	}

	results, err := store.Execute(transactions)
	if err == nil {
		util.Assert(len(transactions) == len(results), "transactions and results must have equal length")
	}

	for i, sqe := range sqes {
		cqe := &bus.CQE[t_aio.Submission, t_aio.Completion]{
			Callback: sqe.Callback,
		}

		if err != nil {
			slog.Error("failed store execution", "err", err)
			cqe.Error = err
		} else {
			cqe.Completion = &t_aio.Completion{
				Kind: t_aio.Store,
				Tags: sqe.Submission.Tags, // propagate the tags
				Store: &t_aio.StoreCompletion{
					Results: results[i],
				},
			}
		}

		cqes = append(cqes, cqe)
	}

	return cqes
}

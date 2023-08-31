package store

import (
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/resonatehq/resonate/internal/util"
)

type Store interface {
	Execute([]*types.Transaction) ([][]*types.Result, error)
}

func Process(store Store, sqes []*bus.SQE[types.Submission, types.Completion]) []*bus.CQE[types.Submission, types.Completion] {
	var cqes []*bus.CQE[types.Submission, types.Completion]
	var transactions []*types.Transaction

	for _, sqe := range sqes {
		util.Assert(sqe.Submission.Store != nil, "submission must not be nil")
		transactions = append(transactions, sqe.Submission.Store.Transaction)
	}

	results, err := store.Execute(transactions)
	if err == nil {
		util.Assert(len(transactions) == len(results), "transactions and results must have equal length")
	}

	for i, sqe := range sqes {
		cqe := &bus.CQE[types.Submission, types.Completion]{
			Kind:     sqe.Kind,
			Callback: sqe.Callback,
		}

		if err != nil {
			cqe.Error = err
		} else {
			cqe.Completion = &types.Completion{
				Kind: types.Store,
				Store: &types.StoreCompletion{
					Results: results[i],
				},
			}
		}

		cqes = append(cqes, cqe)
	}

	return cqes
}

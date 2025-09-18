package store

import (
	"fmt"
	"log/slog"
	"runtime"

	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/util"
)

type Store interface {
	Execute([]*t_aio.Transaction) ([]*t_aio.StoreCompletion, error)
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
			Id:       sqe.Id,
			Callback: sqe.Callback,
		}

		if err != nil {
			slog.Error("failed store execution", "err", err)
			cqe.Error = err
		} else {
			cqe.Completion = &t_aio.Completion{
				Kind:  t_aio.Store,
				Tags:  sqe.Submission.Tags, // propagate the tags
				Store: results[i],
			}
		}

		cqes = append(cqes, cqe)
	}

	return cqes
}

func Collect(c <-chan *bus.SQE[t_aio.Submission, t_aio.Completion], f <-chan int64, n int) ([]*bus.SQE[t_aio.Submission, t_aio.Completion], bool) {
	util.Assert(n > 0, "batch size must be greater than 0")
	batch := []*bus.SQE[t_aio.Submission, t_aio.Completion]{}

	for i := 0; i < n; i++ {
		select {
		case sqe, ok := <-c:
			if !ok {
				return batch, false
			}

			slog.Debug("aio:sqe:dequeue", "id", sqe.Id, "sqe", sqe)
			batch = append(batch, sqe)
		case <-f:
			return batch, true
		}
	}

	return batch, true
}

func StoreErr(err error) error {
	pc, file, loc, ok := runtime.Caller(1)
	details := runtime.FuncForPC(pc)
	if ok && details != nil {
		return fmt.Errorf("%w at %s %s:%d", err, details.Name(), file, loc)
	}

	return err
}

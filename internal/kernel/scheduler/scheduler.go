package scheduler

import (
	"log/slog"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/types"
)

type Scheduler struct {
	aio        aio.AIO
	coroutines []*Coroutine
}

func NewScheduler(aio aio.AIO) *Scheduler {
	return &Scheduler{
		aio:        aio,
		coroutines: []*Coroutine{},
	}
}

func (s *Scheduler) Add(coroutine *Coroutine) {
	slog.Info("scheduler:add", "coroutine", coroutine.String())

	coroutine.init(s, coroutine)
	s.coroutines = append(s.coroutines, coroutine)
}

func (s *Scheduler) Tick(t int64, batchSize int) {
	var coroutines []*Coroutine

	for _, coroutine := range s.coroutines {
		if submission := coroutine.next(); submission != nil {
			sqe := &bus.SQE[types.Submission, types.Completion]{
				Submission: submission,
				Callback:   coroutine.resume,
			}

			slog.Info("aio:enqueue", "sqe", sqe.Submission)
			s.aio.Enqueue(sqe)
		}

		if !coroutine.done() {
			coroutines = append(coroutines, coroutine)
		} else {
			slog.Info("scheduler:rmv", "coroutine", coroutine.String())
		}
	}

	// flush
	s.aio.Flush(t)

	// discard completed coroutines
	s.coroutines = coroutines

	// callback cqes
	for _, cqe := range s.aio.Dequeue(batchSize) {
		slog.Info("aio:dequeue", "cqe", cqe.Completion)
		cqe.Callback(cqe.Completion, cqe.Error)
	}
}

func (s *Scheduler) Done() bool {
	return len(s.coroutines) == 0
}

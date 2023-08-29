package scheduler

import (
	"log/slog"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/resonatehq/resonate/internal/metrics"
)

type Scheduler struct {
	aio        aio.AIO
	metrics    *metrics.Metrics
	coroutines []*Coroutine
}

func NewScheduler(aio aio.AIO, metrics *metrics.Metrics) *Scheduler {
	return &Scheduler{
		aio:        aio,
		metrics:    metrics,
		coroutines: []*Coroutine{},
	}
}

func (s *Scheduler) Add(coroutine *Coroutine) {
	slog.Debug("scheduler:add", "coroutine", coroutine.String())
	s.metrics.CoroutinesTotal.WithLabelValues(coroutine.Kind()).Inc()
	s.metrics.CoroutinesInFlight.WithLabelValues(coroutine.Kind()).Inc()

	coroutine.init(s, coroutine)
	s.coroutines = append(s.coroutines, coroutine)
}

func (s *Scheduler) Tick(t int64, batchSize int) {
	var coroutines []*Coroutine

	for _, coroutine := range s.coroutines {
		if submission := coroutine.next(); submission != nil {
			s.aio.Enqueue(&bus.SQE[types.Submission, types.Completion]{
				Submission: submission,
				Callback:   coroutine.resume,
			})
		}

		if !coroutine.done() {
			coroutines = append(coroutines, coroutine)
		} else {
			s.metrics.CoroutinesInFlight.WithLabelValues(coroutine.Kind()).Dec()
		}
	}

	// flush
	s.aio.Flush(t)

	// discard completed coroutines
	s.coroutines = coroutines

	// callback cqes
	for _, cqe := range s.aio.Dequeue(batchSize) {
		cqe.Callback(cqe.Completion, cqe.Error)
	}
}

func (s *Scheduler) Done() bool {
	return len(s.coroutines) == 0
}

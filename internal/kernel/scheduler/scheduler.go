package scheduler

import (
	"log/slog"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/metrics"
)

type Scheduler struct {
	aio        aio.AIO
	time       int64
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
	slog.Debug("scheduler:add", "coroutine", coroutine.name)
	s.metrics.CoroutinesTotal.WithLabelValues(coroutine.name).Inc()
	s.metrics.CoroutinesInFlight.WithLabelValues(coroutine.name).Inc()

	s.coroutines = append(s.coroutines, coroutine)
}

func (s *Scheduler) Tick(t int64, batchSize int) {
	var coroutines []*Coroutine
	s.time = t

	// dequeue cqes
	for _, cqe := range s.aio.Dequeue(batchSize) {
		cqe.Callback(cqe.Completion, cqe.Error)
	}

	// enqueue cqes
	for _, coroutine := range s.coroutines {
		if !coroutine.initialized {
			coroutine.init(s, coroutine)
			coroutine.initialized = true
		}
		if submission := coroutine.next(); submission != nil {
			s.aio.Enqueue(&bus.SQE[t_aio.Submission, t_aio.Completion]{
				Tags:       submission.Kind.String(),
				Submission: submission,
				Callback:   coroutine.resume,
			})
		}

		if !coroutine.done() {
			coroutines = append(coroutines, coroutine)
		} else {
			slog.Debug("scheduler:rmv", "coroutine", coroutine.name)
			s.metrics.CoroutinesInFlight.WithLabelValues(coroutine.name).Dec()
		}
	}

	// flush
	s.aio.Flush(t)

	// discard completed coroutines
	s.coroutines = coroutines
}

func (s *Scheduler) Time() int64 {
	return s.time
}

func (s *Scheduler) Done() bool {
	return len(s.coroutines) == 0
}

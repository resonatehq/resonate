package scheduler

import (
	"log/slog"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/metrics"
)

type S interface {
	Add(coroutine *Coroutine[*t_aio.Completion, *t_aio.Submission])
	Time() int64
}

type Scheduler struct {
	aio        aio.AIO
	time       int64
	metrics    *metrics.Metrics
	coroutines []*suspendableCoroutine
}

type suspendableCoroutine struct {
	*Coroutine[*t_aio.Completion, *t_aio.Submission]
	next      *t_aio.Completion
	error     error
	suspended bool
}

func NewScheduler(aio aio.AIO, metrics *metrics.Metrics) *Scheduler {
	return &Scheduler{
		aio:     aio,
		metrics: metrics,
	}
}

func (s *Scheduler) Add(coroutine *Coroutine[*t_aio.Completion, *t_aio.Submission]) {
	slog.Debug("scheduler:add", "coroutine", coroutine.name)
	s.metrics.CoroutinesTotal.WithLabelValues(coroutine.name).Inc()
	s.metrics.CoroutinesInFlight.WithLabelValues(coroutine.name).Inc()

	// add reference to scheduler
	coroutine.Scheduler = s

	// wrap in suspendable coroutine
	s.coroutines = append(s.coroutines, &suspendableCoroutine{
		Coroutine: coroutine,
	})
}

func (s *Scheduler) Tick(t int64, batchSize int) {
	coroutines := []*suspendableCoroutine{}
	s.time = t

	// dequeue cqes
	for _, cqe := range s.aio.Dequeue(batchSize) {
		cqe.Callback(cqe.Completion, cqe.Error)
	}

	// enqueue sqes
	for _, coroutine := range s.coroutines {
		if coroutine.suspended {
			continue
		}

		if submission := coroutine.Resume(coroutine.next, coroutine.error); !submission.Done {
			s.aio.Enqueue(&bus.SQE[t_aio.Submission, t_aio.Completion]{
				Tags:       submission.Value.Kind.String(),
				Submission: submission.Value,
				Callback: func(completion *t_aio.Completion, err error) {
					// unsuspend
					coroutine.next = completion
					coroutine.error = err
					coroutine.suspended = false
				},
			})

			// suspend
			coroutine.suspended = true
			coroutines = append(coroutines, coroutine)
		} else {
			// call onDone functions
			for _, f := range coroutine.onDone {
				f()
			}

			slog.Debug("scheduler:rmv", "coroutine", coroutine.name)
			s.metrics.CoroutinesInFlight.WithLabelValues(coroutine.name).Dec()
		}
	}

	// flush
	s.aio.Flush(t)

	// discard done coroutines
	s.coroutines = coroutines
}

func (s *Scheduler) Time() int64 {
	return s.time
}

func (s *Scheduler) Done() bool {
	return len(s.coroutines) == 0
}

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
	aio       aio.AIO
	time      int64
	metrics   *metrics.Metrics
	runnable  []*runnableCoroutine
	suspended []*Coroutine[*t_aio.Completion, *t_aio.Submission]
}

type runnableCoroutine struct {
	*Coroutine[*t_aio.Completion, *t_aio.Submission]
	next  *t_aio.Completion
	error error
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
	s.runnable = append(s.runnable, &runnableCoroutine{
		Coroutine: coroutine,
	})
}

func (s *Scheduler) Tick(t int64, batchSize int) {
	s.time = t

	// dequeue cqes
	for _, cqe := range s.aio.Dequeue(batchSize) {
		cqe.Callback(cqe.Completion, cqe.Error)
	}

	// enqueue sqes
	for _, coroutine := range s.runnable {
		coroutine := coroutine // bind to local variable for callback

		if submission := coroutine.Resume(coroutine.next, coroutine.error); !submission.Done {
			// suspend
			s.suspended = append(s.suspended, coroutine.Coroutine)

			s.aio.Enqueue(&bus.SQE[t_aio.Submission, t_aio.Completion]{
				Tags:       submission.Value.Kind.String(),
				Submission: submission.Value,
				Callback: func(completion *t_aio.Completion, err error) {
					// unsuspend
					s.runnable = append(s.runnable, &runnableCoroutine{
						Coroutine: coroutine.Coroutine,
						next:      completion,
						error:     err,
					})

					for i, c := range s.suspended {
						if c == coroutine.Coroutine {
							s.suspended = append(s.suspended[:i], s.suspended[i+1:]...)
							break
						}
					}
				},
			})
		} else {
			slog.Debug("scheduler:rmv", "coroutine", coroutine.name)
			s.metrics.CoroutinesInFlight.WithLabelValues(coroutine.name).Dec()

			// call onDone functions
			for _, f := range coroutine.onDone {
				f()
			}
		}
	}

	// flush
	s.aio.Flush(t)

	// clear runnable
	s.runnable = nil
}

func (s *Scheduler) Time() int64 {
	return s.time
}

func (s *Scheduler) Done() bool {
	return len(s.runnable) == 0 && len(s.suspended) == 0
}

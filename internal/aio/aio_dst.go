package aio

import (
	"fmt"
	"log/slog"
	"math/rand" // nosemgrep

	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/metrics"

	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/util"
)

// create a DST fault injection error
type AioDSTError struct {
	msg string
}

func (e *AioDSTError) Error() string {
	return e.msg
}

func (e *AioDSTError) Is(target error) bool {
	_, ok := target.(*AioDSTError)
	return ok
}

type aioDST struct {
	r          *rand.Rand
	p          float64
	sqes       []*bus.SQE[t_aio.Submission, t_aio.Completion]
	cqes       []*bus.CQE[t_aio.Submission, t_aio.Completion]
	subsystems map[t_aio.Kind]Subsystem
	metrics    *metrics.Metrics
}

func NewDST(r *rand.Rand, p float64, metrics *metrics.Metrics) *aioDST {
	return &aioDST{
		r:          r,
		p:          p,
		subsystems: map[t_aio.Kind]Subsystem{},
		metrics:    metrics,
	}
}

func (a *aioDST) AddSubsystem(kind t_aio.Kind, subsystem Subsystem, config *SubsystemConfig) {
	a.subsystems[kind] = subsystem
}

func (a *aioDST) Start() error {
	for _, subsystem := range util.OrderedRange(a.subsystems) {
		if err := subsystem.Start(); err != nil {
			return err
		}
	}

	return nil
}

func (a *aioDST) Stop() error {
	for _, subsystem := range util.OrderedRange(a.subsystems) {
		if err := subsystem.Stop(); err != nil {
			return err
		}
	}

	return nil
}

func (a *aioDST) Shutdown() {}

func (a *aioDST) Errors() <-chan error {
	return nil
}

func (a *aioDST) CQ() <-chan *bus.CQE[t_aio.Submission, t_aio.Completion] {
	panic("not implemented")
}

func (a *aioDST) Enqueue(submission *t_aio.Submission, callback func(*t_aio.Completion, error)) {
	sqe := &bus.SQE[t_aio.Submission, t_aio.Completion]{
		Submission: submission,
		Callback: func(completion *t_aio.Completion, err error) {
			var status string
			if err != nil {
				status = "failure"
			} else {
				status = "success"
			}

			a.metrics.AioTotal.WithLabelValues(completion.Kind.String(), status).Inc()
			a.metrics.AioInFlight.WithLabelValues(completion.Kind.String()).Dec()

			callback(completion, err)
		},
	}

	slog.Debug("aio:enqueue", "id", submission.Id(), "sqe", sqe)
	a.metrics.AioInFlight.WithLabelValues(submission.Kind.String()).Inc()

	i := a.r.Intn(len(a.sqes) + 1)
	a.sqes = append(a.sqes[:i], append([]*bus.SQE[t_aio.Submission, t_aio.Completion]{sqe}, a.sqes[i:]...)...)
}

func (a *aioDST) Dequeue(n int) []*bus.CQE[t_aio.Submission, t_aio.Completion] {
	cqes := a.cqes[:min(n, len(a.cqes))]
	a.cqes = a.cqes[min(n, len(a.cqes)):]

	return cqes
}

func (a *aioDST) Flush(t int64) {
	flush := map[t_aio.Kind][]*bus.SQE[t_aio.Submission, t_aio.Completion]{}
	for _, sqe := range a.sqes {
		flush[sqe.Submission.Kind] = append(flush[sqe.Submission.Kind], sqe)
	}

	for _, sqes := range util.OrderedRangeKV(flush) {
		if subsystem, ok := a.subsystems[sqes.Key]; ok {
			// maintain a list of sqes that were not marked as errors
			_sqes := []*bus.SQE[t_aio.Submission, t_aio.Completion]{}

			for i := range sqes.Value {
				// Simulate failure before processing
				if a.r.Float64() < a.p {
					cqe := &bus.CQE[t_aio.Submission, t_aio.Completion]{
						Completion: nil,
						Callback:   sqes.Value[i].Callback,
						Error:      &AioDSTError{msg: "aio dst: failure, before processing"},
					}
					a.cqes = append(a.cqes, cqe)
				} else {
					_sqes = append(_sqes, sqes.Value[i])

				}
			}

			// Process the SQE
			cqes := subsystem.NewWorker(0).Process(_sqes)

			// Simulate failure after processing
			if a.r.Float64() < a.p {
				cqes[0].Completion = nil
				cqes[0].Error = &AioDSTError{msg: "aio dst: failure, after processing"}
			}

			a.cqes = append(a.cqes, cqes...)
		} else {
			panic("invalid aio submission")
		}
	}

	a.sqes = nil
}

func (a *aioDST) String() string {
	// use subsystem keys so that we can compare cross-store dst runs
	subsystems := make([]t_aio.Kind, len(a.subsystems))
	for i, subsystem := range util.OrderedRangeKV[t_aio.Kind](a.subsystems) {
		subsystems[i] = subsystem.Key
	}

	return fmt.Sprintf("AIODST(subsystems=%s)", subsystems)
}

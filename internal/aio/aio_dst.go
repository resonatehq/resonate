package aio

import (
	"errors"
	"fmt"
	"math/rand" // nosemgrep

	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/metrics"

	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/util"
)

type aioDST struct {
	r          *rand.Rand
	p          float64
	sqes       []*bus.SQE[t_aio.Submission, t_aio.Completion]
	cqes       []*bus.CQE[t_aio.Submission, t_aio.Completion]
	subsystems map[t_aio.Kind]SubsystemDST
	metrics    *metrics.Metrics
}

func NewDST(r *rand.Rand, p float64, metrics *metrics.Metrics) *aioDST {
	return &aioDST{
		r:          r,
		p:          p,
		subsystems: map[t_aio.Kind]SubsystemDST{},
		metrics:    metrics,
	}
}

func (a *aioDST) String() string {
	// use subsystem keys so that we can compare cross-store dst runs
	subsystems := make([]t_aio.Kind, len(a.subsystems))
	for i, subsystem := range util.OrderedRangeKV(a.subsystems) {
		subsystems[i] = subsystem.Key
	}

	return fmt.Sprintf("AIODST(subsystems=%s)", subsystems)
}

func (a *aioDST) AddSubsystem(subsystem SubsystemDST) {
	a.subsystems[subsystem.Kind()] = subsystem
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

func (a *aioDST) Signal(cancel <-chan interface{}) <-chan interface{} {
	panic("not implemented")
}

func (a *aioDST) Dispatch(submission *t_aio.Submission, callback func(*t_aio.Completion, error)) {
	sqe := &bus.SQE[t_aio.Submission, t_aio.Completion]{
		Submission: submission,
		Callback: func(completion *t_aio.Completion, err error) {
			util.Assert(completion != nil && err == nil || completion == nil && err != nil, "one of completion/err must be set")

			var status string
			if err != nil {
				status = "failure"
			} else {
				status = "success"
			}

			a.metrics.AioTotal.WithLabelValues(submission.Kind.String(), status).Inc()
			a.metrics.AioInFlight.WithLabelValues(submission.Kind.String()).Dec()

			callback(completion, err)
		},
	}

	a.metrics.AioInFlight.WithLabelValues(submission.Kind.String()).Inc()

	// insert at random position
	i := a.r.Intn(len(a.sqes) + 1)
	a.sqes = append(a.sqes[:i], append([]*bus.SQE[t_aio.Submission, t_aio.Completion]{sqe}, a.sqes[i:]...)...)
}

func (a *aioDST) Enqueue(cqe *bus.CQE[t_aio.Submission, t_aio.Completion]) {
	a.cqes = append(a.cqes, cqe)
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
			toProcess := []*bus.SQE[t_aio.Submission, t_aio.Completion]{}

			preFailure, postFailure, n := map[int]bool{}, map[int]bool{}, 0
			for i := range sqes.Value {
				// there is a p percent chance of failure
				// either pre or post processing
				if a.r.Float64() < a.p {
					switch a.r.Intn(2) {
					case 0:
						preFailure[i] = true
					case 1:
						postFailure[n] = true
					}
				}

				if preFailure[i] {
					// simulate failure before processing
					a.Enqueue(&bus.CQE[t_aio.Submission, t_aio.Completion]{
						Completion: nil,
						Callback:   sqes.Value[i].Callback,
						Error:      errors.New("simulated failure before processing"),
					})
				} else {
					toProcess = append(toProcess, sqes.Value[i])
					n++
				}
			}

			// process the SQE
			for i, cqe := range subsystem.Process(toProcess) {
				if postFailure[i] {
					// simulate failure after processing
					cqe.Completion = nil
					cqe.Error = errors.New("simulated failure after processing")
				}
				a.Enqueue(cqe)
			}
		} else {
			panic("invalid aio submission")
		}
	}

	a.sqes = nil
}

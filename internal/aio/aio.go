package aio

import (
	"fmt"
	"log/slog"

	"github.com/resonatehq/gocoro/pkg/io"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"

	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/resonatehq/resonate/internal/util"
)

type AIO interface {
	String() string
	AddSubsystem(subsystem Subsystem)
	Start() error
	Stop() error
	Shutdown()
	CQ() <-chan *bus.CQE[t_aio.Submission, t_aio.Completion]
	Enqueue(*t_aio.Submission, func(*t_aio.Completion, error))
	Dequeue(int) []io.QE
	Flush(int64)
	Errors() <-chan error
}

type aio struct {
	cq         chan *bus.CQE[t_aio.Submission, t_aio.Completion]
	subsystems map[t_aio.Kind]Subsystem
	errors     chan error
	metrics    *metrics.Metrics
}

func New(cq chan *bus.CQE[t_aio.Submission, t_aio.Completion], metrics *metrics.Metrics) *aio {
	return &aio{
		cq:         cq,
		subsystems: map[t_aio.Kind]Subsystem{},
		errors:     make(chan error),
		metrics:    metrics,
	}
}

func (a *aio) AddSubsystem(subsystem Subsystem) {
	a.subsystems[subsystem.Kind()] = subsystem
}

func (a *aio) Start() error {
	for _, subsystem := range util.OrderedRange(a.subsystems) {
		if err := subsystem.Start(); err != nil {
			return err
		}
	}

	return nil
}

func (a *aio) Stop() error {
	defer close(a.cq)

	for _, subsystem := range util.OrderedRange(a.subsystems) {
		if err := subsystem.Stop(); err != nil {
			return err
		}

		close(subsystem.SQ())
	}

	return nil
}

func (a *aio) Shutdown() {}

func (a *aio) Errors() <-chan error {
	return a.errors
}

func (a *aio) CQ() <-chan *bus.CQE[t_aio.Submission, t_aio.Completion] {
	return a.cq
}

func (a *aio) Enqueue(submission *t_aio.Submission, callback func(*t_aio.Completion, error)) {
	util.Assert(submission.Tags != nil, "submission tags must be set")

	if subsystem, ok := a.subsystems[submission.Kind]; ok {
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

		select {
		case subsystem.SQ() <- sqe:
			slog.Debug("aio:enqueue", "id", submission.Id(), "sqe", sqe)
			a.metrics.AioInFlight.WithLabelValues(submission.Kind.String()).Inc()
		default:
			sqe.Callback(nil, t_api.NewResonateError(t_api.ErrAIOSubmissionQueueFull, fmt.Sprintf("aio:subsytem:%s submission queue full", subsystem), nil))
		}
	} else {
		panic("invalid aio submission")
	}
}

func (a *aio) Dequeue(n int) []io.QE {
	cqes := []io.QE{}

	// collects n entries or until the channel is
	// exhausted, whichever happens first
	for i := 0; i < n; i++ {
		select {
		case cqe, ok := <-a.cq:
			if !ok {
				return cqes
			}

			slog.Debug("aio:dequeue", "id", cqe.Completion.Id(), "cqe", cqe)
			cqes = append(cqes, cqe)
		default:
			return cqes
		}
	}

	return cqes
}

func (a *aio) Flush(t int64) {
	for _, subsystem := range util.OrderedRange(a.subsystems) {
		subsystem.Flush(t)
	}
}

func (a *aio) String() string {
	return fmt.Sprintf(
		"AIO(size=%d, subsystems=%s)",
		cap(a.cq),
		a.subsystems,
	)
}

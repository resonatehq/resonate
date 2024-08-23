package aio

import (
	"fmt"
	"log/slog"

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
	Errors() <-chan error

	Signal(<-chan interface{}) <-chan interface{}
	Flush(int64)

	Dispatch(*t_aio.Submission, func(*t_aio.Completion, error))
	Enqueue(*bus.CQE[t_aio.Submission, t_aio.Completion])
	Dequeue(int) []*bus.CQE[t_aio.Submission, t_aio.Completion]
}

// AIO

type aio struct {
	cq         chan *bus.CQE[t_aio.Submission, t_aio.Completion]
	buffer     *bus.CQE[t_aio.Submission, t_aio.Completion]
	subsystems map[t_aio.Kind]Subsystem
	errors     chan error
	metrics    *metrics.Metrics
}

func New(size int, metrics *metrics.Metrics) *aio {
	return &aio{
		cq:         make(chan *bus.CQE[t_aio.Submission, t_aio.Completion], size),
		subsystems: map[t_aio.Kind]Subsystem{},
		errors:     make(chan error),
		metrics:    metrics,
	}
}

func (a *aio) String() string {
	return fmt.Sprintf(
		"AIO(size=%d, subsystems=%s)",
		cap(a.cq),
		a.subsystems,
	)
}

func (a *aio) AddSubsystem(subsystem Subsystem) {
	a.subsystems[subsystem.Kind()] = subsystem
}

// Lifecycle functions

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
	}

	return nil
}

func (a *aio) Shutdown() {}

func (a *aio) Errors() <-chan error {
	return a.errors
}

// IO functions

func (a *aio) Signal(cancel <-chan interface{}) <-chan interface{} {
	ch := make(chan interface{})

	if a.buffer != nil {
		close(ch)
		return ch
	}

	go func() {
		defer close(ch)

		select {
		case cqe := <-a.cq:
			util.Assert(a.buffer == nil, "buffer must be nil")
			a.buffer = cqe
		case <-cancel:
			break
		}
	}()

	return ch
}

func (a *aio) Dispatch(submission *t_aio.Submission, callback func(*t_aio.Completion, error)) {
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
			a.metrics.AioInFlight.WithLabelValues(submission.Kind.String()).Inc()
		default:
			sqe.Callback(nil, t_api.NewResonateError(t_api.ErrAIOSubmissionQueueFull, fmt.Sprintf("aio:subsytem:%s submission queue full", subsystem), nil))
		}
	} else {
		panic("invalid aio submission")
	}
}

func (a *aio) Enqueue(cqe *bus.CQE[t_aio.Submission, t_aio.Completion]) {
	util.Assert(cqe != nil, "cqe must not be nil")

	// block until the completion queue has space
	a.cq <- cqe
	slog.Debug("aio:enqueue", "id", cqe.Completion.Id(), "cqe", cqe)
}

func (a *aio) Dequeue(n int) []*bus.CQE[t_aio.Submission, t_aio.Completion] {
	cqes := []*bus.CQE[t_aio.Submission, t_aio.Completion]{}

	// insert the buffered sqe
	if a.buffer != nil {
		slog.Debug("aio:dequeue", "id", a.buffer.Completion.Id(), "cqe", a.buffer)
		cqes = append(cqes, a.buffer)
		a.buffer = nil
	}

	// collects n entries (if immediately available)
	for i := 0; i < n-len(cqes); i++ {
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

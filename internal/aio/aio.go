package aio

import (
	"fmt"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"

	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/resonatehq/resonate/internal/util"
)

type AIO interface {
	String() string

	Start() error
	Stop() error
	Shutdown()
	Errors() <-chan error

	Signal(<-chan interface{}) <-chan interface{}
	Flush(int64)

	// dispatch is required by gocoro
	Dispatch(*t_aio.Submission, func(*t_aio.Completion, error))

	EnqueueSQE(*bus.SQE[t_aio.Submission, t_aio.Completion])
	EnqueueCQE(*bus.CQE[t_aio.Submission, t_aio.Completion])
	DequeueCQE(int) []*bus.CQE[t_aio.Submission, t_aio.Completion]
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
		if err := subsystem.Start(a.errors); err != nil {
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

func (a *aio) Signal(cancel <-chan any) <-chan any {
	ch := make(chan any)

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

func (a *aio) Flush(t int64) {
	for _, subsystem := range util.OrderedRange(a.subsystems) {
		subsystem.Flush(t)
	}
}

// SQE

func (a *aio) Dispatch(submission *t_aio.Submission, callback func(*t_aio.Completion, error)) {
	util.Assert(submission.Tags != nil, "submission tags must be non nil")
	util.Assert(submission.Tags["id"] != "", "id tag must be set")

	a.EnqueueSQE(&bus.SQE[t_aio.Submission, t_aio.Completion]{
		Id:         submission.Tags["id"],
		Submission: submission,
		Callback:   callback,
	})
}

func (a *aio) EnqueueSQE(sqe *bus.SQE[t_aio.Submission, t_aio.Completion]) {
	subsystem, ok := a.subsystems[sqe.Submission.Kind]
	if !ok {
		panic("invalid aio submission")
	}

	slog.Debug("aio:sqe:enqueue", "id", sqe.Id, "sqe", sqe)
	kind := sqe.Submission.Kind.String()
	timer := prometheus.NewTimer(a.metrics.AioDuration.WithLabelValues(kind))
	count := a.metrics.AioInFlight.WithLabelValues(kind)
	count.Inc()

	callback := sqe.Callback
	sqe.Callback = func(completion *t_aio.Completion, err error) {
		util.Assert(completion != nil && err == nil || completion == nil && err != nil, "one of completion/err must be set")

		var status string
		if err != nil {
			status = "failure"
		} else {
			status = "success"
		}

		timer.ObserveDuration()
		count.Dec()
		a.metrics.AioTotal.WithLabelValues(sqe.Submission.Kind.String(), status).Inc()

		callback(completion, err)
	}

	if !subsystem.Enqueue(sqe) {
		sqe.Callback(nil, t_api.NewError(t_api.StatusAIOSubmissionQueueFull, nil))
	}
}

// CQE

func (a *aio) EnqueueCQE(cqe *bus.CQE[t_aio.Submission, t_aio.Completion]) {
	util.Assert(cqe != nil, "cqe must not be nil")

	// block until the completion queue has space
	slog.Debug("aio:cqe:enqueue", "id", cqe.Id, "cqe", cqe)
	a.cq <- cqe
}

func (a *aio) DequeueCQE(n int) []*bus.CQE[t_aio.Submission, t_aio.Completion] {
	cqes := []*bus.CQE[t_aio.Submission, t_aio.Completion]{}

	// insert the buffered sqe
	if a.buffer != nil {
		slog.Debug("aio:cqe:dequeue", "id", a.buffer.Id, "cqe", a.buffer)
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

			slog.Debug("aio:cqe:dequeue", "id", cqe.Id, "cqe", cqe)
			cqes = append(cqes, cqe)
		default:
			return cqes
		}
	}

	return cqes
}

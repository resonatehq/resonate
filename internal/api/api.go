package api

import (
	"errors"
	"fmt"
	"strconv"

	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/resonatehq/resonate/internal/util"
)

type API interface {
	String() string
	AddSubsystem(subsystem Subsystem)

	Start() error
	Stop() error
	Shutdown()
	Done() bool
	Errors() <-chan error

	Signal(<-chan interface{}) <-chan interface{}
	Enqueue(*bus.SQE[t_api.Request, t_api.Response])
	Dequeue(int) []*bus.SQE[t_api.Request, t_api.Response]
}

// API

type api struct {
	sq         chan *bus.SQE[t_api.Request, t_api.Response]
	buffer     *bus.SQE[t_api.Request, t_api.Response]
	subsystems []Subsystem
	done       bool
	errors     chan error
	metrics    *metrics.Metrics
}

func New(size int, metrics *metrics.Metrics) API {
	return &api{
		sq:      make(chan *bus.SQE[t_api.Request, t_api.Response], size),
		errors:  make(chan error),
		metrics: metrics,
	}
}

func (a *api) String() string {
	return fmt.Sprintf(
		"API(size=%d, subsystems=%s)",
		cap(a.sq),
		a.subsystems,
	)
}

func (a *api) AddSubsystem(subsystem Subsystem) {
	a.subsystems = append(a.subsystems, subsystem)
}

// Lifecycle functions

func (a *api) Start() error {
	for _, subsystem := range a.subsystems {
		go subsystem.Start(a.errors)
	}

	return nil
}

func (a *api) Stop() error {
	defer close(a.sq)

	for _, subsystem := range a.subsystems {
		if err := subsystem.Stop(); err != nil {
			return err
		}
	}

	return nil
}

func (a *api) Shutdown() {
	a.done = true
}

func (a *api) Done() bool {
	return a.done && len(a.sq) == 0
}

func (a *api) Errors() <-chan error {
	return a.errors
}

// IO functions

func (a *api) Signal(cancel <-chan interface{}) <-chan interface{} {
	ch := make(chan interface{})

	if a.buffer != nil {
		close(ch)
		return ch
	}

	go func() {
		defer close(ch)

		select {
		case sqe := <-a.sq:
			util.Assert(a.buffer == nil, "buffer must be nil")
			a.buffer = sqe
		case <-cancel:
			break
		}
	}()

	return ch
}

func (a *api) Enqueue(sqe *bus.SQE[t_api.Request, t_api.Response]) {
	util.Assert(sqe.Submission != nil, "submission must not be nil")
	util.Assert(sqe.Submission.Tags != nil, "request tags must not be nil")
	a.metrics.ApiInFlight.WithLabelValues(sqe.Submission.Kind.String(), sqe.Submission.Tags["protocol"]).Inc()

	// replace callback with a function that emits metrics
	callback := sqe.Callback
	sqe.Callback = func(res *t_api.Response, err error) {
		util.Assert(res != nil && err == nil || res == nil && err != nil, "one of res/err must be set")
		var status t_api.StatusCode

		if err != nil {
			var error *t_api.Error
			util.Assert(errors.As(err, &error), "err must be a ResonateError")

			status = error.Code()
		} else {
			status = res.Status()
		}

		a.metrics.ApiTotal.WithLabelValues(sqe.Submission.Kind.String(), sqe.Submission.Tags["protocol"], strconv.Itoa(int(status))).Inc()
		a.metrics.ApiInFlight.WithLabelValues(sqe.Submission.Kind.String(), sqe.Submission.Tags["protocol"]).Dec()

		callback(res, err)
	}

	// we must wait to close the channel because even in a select
	// sending to a closed channel will panic
	if a.done {
		sqe.Callback(nil, t_api.NewError(t_api.StatusSystemShuttingDown, nil))
		return
	}

	select {
	case a.sq <- sqe:
		slog.Debug("api:enqueue", "id", sqe.Id, "sqe", sqe)
	default:
		sqe.Callback(nil, t_api.NewError(t_api.StatusAPISubmissionQueueFull, nil))
	}
}

func (a *api) Dequeue(n int) []*bus.SQE[t_api.Request, t_api.Response] {
	sqes := []*bus.SQE[t_api.Request, t_api.Response]{}

	// insert the buffered sqe
	if a.buffer != nil {
		slog.Debug("api:dequeue", "id", a.buffer.Id, "sqe", a.buffer)
		sqes = append(sqes, a.buffer)
		a.buffer = nil
	}

	// collects n entries (if immediately available)
	for i := 0; i < n-len(sqes); i++ {
		select {
		case sqe, ok := <-a.sq:
			if !ok {
				return sqes
			}
			slog.Debug("api:dequeue", "id", sqe.Id, "sqe", sqe)
			sqes = append(sqes, sqe)
		default:
			return sqes
		}
	}

	return sqes
}

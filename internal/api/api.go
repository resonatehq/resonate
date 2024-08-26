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
		var status int

		if err != nil {
			var resErr *t_api.ResonateError
			util.Assert(errors.As(err, &resErr), "err must be a ResonateError")
			status = int(resErr.Code())
		} else {
			switch res.Kind {
			case t_api.ReadPromise:
				status = int(res.ReadPromise.Status)
			case t_api.SearchPromises:
				status = int(res.SearchPromises.Status)
			case t_api.CreatePromise:
				status = int(res.CreatePromise.Status)
			case t_api.CompletePromise:
				status = int(res.CompletePromise.Status)
			case t_api.CreateCallback:
				status = int(res.CreateCallback.Status)
			case t_api.ReadSchedule:
				status = int(res.ReadSchedule.Status)
			case t_api.SearchSchedules:
				status = int(res.SearchSchedules.Status)
			case t_api.CreateSchedule:
				status = int(res.CreateSchedule.Status)
			case t_api.DeleteSchedule:
				status = int(res.DeleteSchedule.Status)
			case t_api.AcquireLock:
				status = int(res.AcquireLock.Status)
			case t_api.ReleaseLock:
				status = int(res.ReleaseLock.Status)
			case t_api.HeartbeatLocks:
				status = int(res.HeartbeatLocks.Status)
			case t_api.ClaimTask:
				status = int(res.ClaimTask.Status)
			case t_api.CompleteTask:
				status = int(res.CompleteTask.Status)
			case t_api.HeartbeatTasks:
				status = int(res.HeartbeatTasks.Status)
			case t_api.Echo:
				status = 2000
			default:
				panic(fmt.Errorf("unknown response kind: %s", res.Kind))
			}
		}

		a.metrics.ApiTotal.WithLabelValues(sqe.Submission.Kind.String(), sqe.Submission.Tags["protocol"], strconv.Itoa(status)).Inc()
		a.metrics.ApiInFlight.WithLabelValues(sqe.Submission.Kind.String(), sqe.Submission.Tags["protocol"]).Dec()

		callback(res, err)
	}

	// we must wait to close the channel because even in a select
	// sending to a closed channel will panic
	if a.done {
		sqe.Callback(nil, t_api.NewResonateError(t_api.ErrSystemShuttingDown, "system is shutting down", nil))
		return
	}

	select {
	case a.sq <- sqe:
		slog.Debug("api:enqueue", "id", sqe.Submission.Id(), "sqe", sqe)
	default:
		sqe.Callback(nil, t_api.NewResonateError(t_api.ErrAPISubmissionQueueFull, "api submission queue is full", nil))
	}
}

func (a *api) Dequeue(n int) []*bus.SQE[t_api.Request, t_api.Response] {
	sqes := []*bus.SQE[t_api.Request, t_api.Response]{}

	// insert the buffered sqe
	if a.buffer != nil {
		slog.Debug("api:dequeue", "id", a.buffer.Submission.Id(), "sqe", a.buffer)
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
			slog.Debug("api:dequeue", "id", sqe.Submission.Id(), "sqe", sqe)
			sqes = append(sqes, sqe)
		default:
			return sqes
		}
	}

	return sqes
}

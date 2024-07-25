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
	SQ() <-chan *bus.SQE[t_api.Request, t_api.Response]
	Enqueue(*t_api.Request, func(*t_api.Response, error))
	Dequeue(int) []*bus.SQE[t_api.Request, t_api.Response]
	Start() error
	Stop() error
	Shutdown()
	Done() bool
	Errors() <-chan error
}

type api struct {
	sq         chan *bus.SQE[t_api.Request, t_api.Response]
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

func (a *api) AddSubsystem(subsystem Subsystem) {
	a.subsystems = append(a.subsystems, subsystem)
}

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

func (a *api) SQ() <-chan *bus.SQE[t_api.Request, t_api.Response] {
	return a.sq
}

func (a *api) Enqueue(request *t_api.Request, callback func(*t_api.Response, error)) {
	util.Assert(request.Tags != nil, "request tags must be set")
	a.metrics.ApiInFlight.WithLabelValues(request.Kind.String(), request.Tags["protocol"]).Inc()

	sqe := &bus.SQE[t_api.Request, t_api.Response]{
		Submission: request,

		// replace callback with a function that emits metrics
		Callback: func(res *t_api.Response, err error) {
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
				case t_api.Echo:
					status = 2000
				default:
					panic(fmt.Errorf("unknown response kind: %d", res.Kind))
				}
			}

			a.metrics.ApiTotal.WithLabelValues(request.Kind.String(), request.Tags["protocol"], strconv.Itoa(status)).Inc()
			a.metrics.ApiInFlight.WithLabelValues(request.Kind.String(), request.Tags["protocol"]).Dec()

			callback(res, err)
		},
	}

	// we must wait to close the channel because even in a select
	// sending to a closed channel will panic
	if a.done {
		sqe.Callback(nil, t_api.NewResonateError(t_api.ErrSystemShuttingDown, "system is shutting down", nil))
		return
	}

	select {
	case a.sq <- sqe:
		slog.Debug("api:enqueue", "id", request.Id(), "sqe", sqe)
	default:
		sqe.Callback(nil, t_api.NewResonateError(t_api.ErrAPISubmissionQueueFull, "api submission queue is full", nil))
	}
}

func (a *api) Dequeue(n int) []*bus.SQE[t_api.Request, t_api.Response] {
	sqes := []*bus.SQE[t_api.Request, t_api.Response]{}

	// collects n entries (if immediately available) or until a timeout occurs,
	// whichever happens first
	for i := 0; i < n-1; i++ {
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

func (a *api) String() string {
	return fmt.Sprintf(
		"API(size=%d, subsystems=%s)",
		cap(a.sq),
		a.subsystems,
	)
}

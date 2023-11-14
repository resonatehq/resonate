package api

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"log/slog"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/metrics"
)

var (
	ErrAPISubmissionQueueFull = errors.New("api submission queue full")
	ErrSystemShuttingDown     = errors.New("system is shutting down")
)

type API interface {
	String() string
	Enqueue(*bus.SQE[t_api.Request, t_api.Response])
	Dequeue(int, <-chan time.Time) []*bus.SQE[t_api.Request, t_api.Response]
	Shutdown()
	Done() bool
}

type api struct {
	sq         chan *bus.SQE[t_api.Request, t_api.Response]
	subsystems []Subsystem
	done       bool
	errors     chan error
	metrics    *metrics.Metrics
}

func New(size int, metrics *metrics.Metrics) *api {
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

func (a *api) Enqueue(sqe *bus.SQE[t_api.Request, t_api.Response]) {
	tags := sqe.Metadata.Tags.Split("api")
	a.metrics.ApiInFlight.WithLabelValues(tags...).Inc()

	// replace sqe.Callback with a callback that wraps the original
	// function and emits metrics
	callback := sqe.Callback
	sqe.Callback = func(res *t_api.Response, err error) {
		var status int

		if err != nil {
			// platform level error - tolerable, but expected
			if errors.Is(err, ErrAPISubmissionQueueFull) {
				status = int(t_api.StatusAPISubmissionQueueFull)
			} else if errors.Is(err, ErrSystemShuttingDown) {
				status = int(t_api.StatusSystemShuttingDown)
			} else if errors.Is(err, aio.ErrAIOSubsytemNetworkSubmissionQueueFull) {
				status = int(t_api.StatusAIONetworkSubmissionQueueFull)
			} else if errors.Is(err, aio.ErrAIOSubsytemStoreSubmissionQueueFull) {
				status = int(t_api.StatusAIOStoreSubmissionQueueFull)
			} else {
				// should know every platform level error that can happen here
				// -- too  strict for now, but good to go in the future
				panic(err)
			}
		} else {
			// application level error - tolerable, but expected
			switch res.Kind {
			case t_api.ReadPromise:
				status = int(res.ReadPromise.Status)
			case t_api.SearchPromises:
				status = int(res.SearchPromises.Status)
			case t_api.CreatePromise:
				status = int(res.CreatePromise.Status)
			case t_api.CancelPromise:
				status = int(res.CancelPromise.Status)
			case t_api.ResolvePromise:
				status = int(res.ResolvePromise.Status)
			case t_api.RejectPromise:
				status = int(res.RejectPromise.Status)
			case t_api.ReadSubscriptions:
				status = int(res.ReadSubscriptions.Status)
			case t_api.CreateSubscription:
				status = int(res.CreateSubscription.Status)
			case t_api.DeleteSubscription:
				status = int(res.DeleteSubscription.Status)
			default:
				panic(fmt.Errorf("unknown response kind: %v", res.Kind))
			}
		}

		a.metrics.ApiTotal.WithLabelValues(append(tags, strconv.Itoa(status))...).Inc()
		a.metrics.ApiInFlight.WithLabelValues(tags...).Dec()

		callback(res, err)
	}

	// we must wait to close the channel because even in a select
	// sending to a closed channel will panic
	if a.done {
		sqe.Callback(nil, ErrSystemShuttingDown)
		return
	}

	select {
	case a.sq <- sqe:
		slog.Debug("api:enqueue", "sqe", sqe)
	default:
		sqe.Callback(nil, ErrAPISubmissionQueueFull)
	}
}

func (a *api) Dequeue(n int, timeoutCh <-chan time.Time) []*bus.SQE[t_api.Request, t_api.Response] {
	sqes := []*bus.SQE[t_api.Request, t_api.Response]{}

	if timeoutCh != nil {
		// collects n entries (if immediately available) or until a timeout occurs,
		// whichever happens first
		select {
		case sqe, ok := <-a.sq:
			if !ok {
				return sqes
			}
			slog.Debug("api:dequeue", "sqe", sqe)
			sqes = append(sqes, sqe)

			for i := 0; i < n-1; i++ {
				select {
				case sqe, ok := <-a.sq:
					if !ok {
						return sqes
					}
					slog.Debug("api:dequeue", "sqe", sqe)
					sqes = append(sqes, sqe)
				default:
					return sqes
				}
			}
			return sqes
		case <-timeoutCh:
			return sqes
		}
	} else {
		// collects n entries or until the channel is
		// exhausted, whichever happens first
		for i := 0; i < n; i++ {
			select {
			case sqe, ok := <-a.sq:
				if !ok {
					return sqes
				}

				slog.Debug("api:dequeue", "sqe", sqe)
				sqes = append(sqes, sqe)
			default:
				return sqes
			}
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

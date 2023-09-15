package api

import (
	"fmt"
	"strconv"
	"time"

	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/resonatehq/resonate/internal/metrics"
)

type API interface {
	String() string
	Enqueue(*bus.SQE[types.Request, types.Response])
	Dequeue(int, <-chan time.Time) []*bus.SQE[types.Request, types.Response]
	Done() bool
}

type api struct {
	sq         chan *bus.SQE[types.Request, types.Response]
	subsystems []Subsystem
	done       bool
	errors     chan error
	metrics    *metrics.Metrics
}

func New(size int, metrics *metrics.Metrics) *api {
	return &api{
		sq:      make(chan *bus.SQE[types.Request, types.Response], size),
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
	for _, subsystem := range a.subsystems {
		if err := subsystem.Stop(); err != nil {
			return err
		}
	}

	return nil
}

func (a *api) Shutdown() {
	close(a.sq)
}

func (a *api) Done() bool {
	return a.done
}

func (a *api) Errors() <-chan error {
	return a.errors
}

func (a *api) Enqueue(sqe *bus.SQE[types.Request, types.Response]) {
	// replace sqe.Callback with a callback that wraps the original
	// function and emits metrics
	callback := sqe.Callback
	sqe.Callback = func(res *types.Response, err error) {
		var status int

		if err != nil {
			status = 500
		} else {
			switch res.Kind {
			case types.ReadPromise:
				status = int(res.ReadPromise.Status)
			case types.SearchPromises:
				status = int(res.SearchPromises.Status)
			case types.CreatePromise:
				status = int(res.CreatePromise.Status)
			case types.CancelPromise:
				status = int(res.CancelPromise.Status)
			case types.ResolvePromise:
				status = int(res.ResolvePromise.Status)
			case types.RejectPromise:
				status = int(res.RejectPromise.Status)
			case types.ReadSubscriptions:
				status = int(res.ReadSubscriptions.Status)
			case types.CreateSubscription:
				status = int(res.CreateSubscription.Status)
			case types.DeleteSubscription:
				status = int(res.DeleteSubscription.Status)
			}
		}

		a.metrics.ApiTotal.WithLabelValues(sqe.Kind, strconv.Itoa(status)).Inc()
		a.metrics.ApiInFlight.WithLabelValues(sqe.Kind).Dec()

		callback(res, err)
	}

	select {
	case a.sq <- sqe:
		a.metrics.ApiInFlight.WithLabelValues(sqe.Kind).Inc()
	default:
		sqe.Callback(nil, fmt.Errorf("api submission queue full"))
	}
}

func (a *api) Dequeue(n int, timeoutCh <-chan time.Time) []*bus.SQE[types.Request, types.Response] {
	sqes := []*bus.SQE[types.Request, types.Response]{}

	if timeoutCh != nil {
		// collects n entries or until a timeout occurs,
		// whichever happens first
		for i := 0; i < n; i++ {
			select {
			case sqe, ok := <-a.sq:
				if !ok {
					a.done = true
					return sqes
				}
				sqes = append(sqes, sqe)
			case <-timeoutCh:
				return sqes
			}
		}
	} else {
		// collects n entries or until the channel is
		// exhausted, whichever happens first
		for i := 0; i < n; i++ {
			select {
			case sqe, ok := <-a.sq:
				if !ok {
					a.done = true
					return sqes
				}
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

package api

import (
	"fmt"
	"time"

	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/types"
)

type API interface {
	Enqueue(*bus.SQE[types.Request, types.Response])
	Dequeue(int, <-chan time.Time) []*bus.SQE[types.Request, types.Response]
	Done() bool
}

type api struct {
	sq         chan *bus.SQE[types.Request, types.Response]
	subsystems []Subsystem
	done       bool
	errors     chan error
}

func New(size int) *api {
	return &api{
		sq:     make(chan *bus.SQE[types.Request, types.Response], size),
		errors: make(chan error),
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
	select {
	case a.sq <- sqe:
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

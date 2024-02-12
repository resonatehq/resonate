package queuing

import (
	"context"
	"fmt"
	"sync"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/bindings"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/bindings/t_bind"
)

// Config is the configuration for the queuing subsystem.
type Config struct {
	Bindings []*t_bind.Config
}

// QueuingSubsystem is a subsystem that dispatches tasks to user defined bindings.
type QueuingSubsystem struct {
	// bindings is a map of binding names to their binding instances.
	bindings map[string]t_bind.Binding

	// bindingsSQ is a map of binding names to their submission queues.
	bindingsSQ map[string]chan *t_bind.BindingSubmission

	// bindingsWG is a wait group to wait for all bindings to finish before shutting down.
	bindingsWG *sync.WaitGroup

	// ctx is the parent context for all bindings.
	ctx context.Context

	// stop is the context cancel function to send a signal to all bindings to stop.
	stop context.CancelFunc
}

// NewSubsytemOrDie creates a new queuing subsystem with the given config.
func NewSubsytemOrDie(config *Config) *QueuingSubsystem {
	defer func() {
		if r := recover(); r != nil {
			panic(fmt.Sprintf("failed to create queuing subsytem: %v", r))
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	tm := &QueuingSubsystem{
		bindings:   make(map[string]t_bind.Binding, len(config.Bindings)),
		bindingsSQ: make(map[string]chan *t_bind.BindingSubmission, len(config.Bindings)),
		bindingsWG: &sync.WaitGroup{},
		ctx:        ctx,
		stop:       cancel,
	}

	for _, cfg := range config.Bindings {
		tsq := make(chan *t_bind.BindingSubmission, 100) // todo: make this configurable

		tm.bindingsSQ[cfg.Name] = tsq
		tm.bindings[cfg.Name] = bindings.NewBindingOrDie(tsq, cfg)
	}

	return tm
}

// String returns the name of the subsystem.
func (t *QueuingSubsystem) String() string {
	return "queuing:default"
}

// NewWorker creates a new worker for the queuing subsystem with the submission queues for each connection.
func (t *QueuingSubsystem) NewWorker(i int) aio.Worker {
	return &QueuingWorker{
		BindingsSQ: t.bindingsSQ,
		i:          i,
	}
}

// Start dispatches all binding to their own goroutines.
func (t *QueuingSubsystem) Start() error {
	t.bindingsWG.Add(len(t.bindings))

	for _, bind := range t.bindings {
		go func(b t_bind.Binding) {
			defer t.bindingsWG.Done()
			t_bind.Start(t.ctx, b)
		}(bind)
	}

	return nil
}

// Stop cancels the subsystems context and waits for all bindings to finish.
func (t *QueuingSubsystem) Stop() error {
	t.stop()
	t.bindingsWG.Wait()
	return nil
}

// Reset resets the subsystems state.
func (t *QueuingSubsystem) Reset() error {
	return nil
}

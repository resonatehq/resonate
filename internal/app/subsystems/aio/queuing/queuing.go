package queuing

import (
	"context"
	"fmt"
	"sync"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/connections"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/connections/t_conn"
)

// Config is the configuration for the queuing subsystem.
type Config struct {
	Connections []*t_conn.ConnectionConfig
}

// QueuingSubsystem is a subsystem that dispatches tasks to user defined connections.
type QueuingSubsystem struct {
	// router contains the information for routing requests to connections.
	connectionRouter Router

	// connections is a map of connection names to their connection instances.
	connections map[string]t_conn.Connection

	// connectionsSQ is a map of connection names to their submission queues.
	connectionsSQ map[string]chan *t_conn.ConnectionSubmission

	// connectionsWG is a wait group to wait for all connections to finish before shutting down.
	connectionsWG *sync.WaitGroup

	// ctx is the parent context for all connections.
	ctx context.Context

	// stop is the context cancel function to send a signal to all connections to stop.
	stop context.CancelFunc
}

// NewSubsytemOrDie creates a new queuing subsystem with the given config.
func NewSubsytemOrDie(config *Config) *QueuingSubsystem {
	defer func() {
		if r := recover(); r != nil {
			panic(fmt.Sprintf("failed to create queuing subsytem: %v", r))
		}
	}()

	var (
		conns      = make(map[string]t_conn.Connection, len(config.Connections))
		connSQ     = make(map[string]chan *t_conn.ConnectionSubmission, len(config.Connections))
		connRouter = NewRouter()
	)

	for _, cfg := range config.Connections {
		tsq := make(chan *t_conn.ConnectionSubmission, 100) // TODO: make this configurable

		connRouter.Handle(cfg.Pattern, &RouteHandler{ // for routing requests to the appropriate connection
			Connection: cfg.Name,
			Queue:      cfg.Queue,
		})
		// Workaround for no dependency injection in coroutines. (TODO: consider alternatives)
		CoroutineRouter().Handle(cfg.Pattern, &RouteHandler{
			Connection: cfg.Name,
			Queue:      cfg.Queue,
		})
		connSQ[cfg.Name] = tsq                                     // for communication between worker and connection
		conns[cfg.Name] = connections.NewConnectionOrDie(tsq, cfg) // for starting the connection
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &QueuingSubsystem{
		connectionRouter: connRouter,
		connections:      conns,
		connectionsSQ:    connSQ,
		connectionsWG:    &sync.WaitGroup{},
		ctx:              ctx,
		stop:             cancel,
	}
}

// String returns the name of the subsystem.
func (t *QueuingSubsystem) String() string {
	return "queuing:default"
}

// NewWorker creates a new worker for the queuing subsystem with the submission queues for each connection.
func (t *QueuingSubsystem) NewWorker(i int) aio.Worker {
	return &QueuingWorker{
		ConnectionRouter: t.connectionRouter,
		ConnectionsSQ:    t.connectionsSQ,
		i:                i,
	}
}

// Start dispatches all connection to their own goroutines.
func (t *QueuingSubsystem) Start() error {
	t.connectionsWG.Add(len(t.connections))

	for _, bind := range t.connections {
		go func(b t_conn.Connection) {
			defer t.connectionsWG.Done()
			t_conn.Start(t.ctx, b)
		}(bind)
	}

	return nil
}

// Stop cancels the subsystems context and waits for all connections to finish.
func (t *QueuingSubsystem) Stop() error {
	t.stop()
	t.connectionsWG.Wait()
	return nil
}

// Reset resets the subsystems state.
func (t *QueuingSubsystem) Reset() error {
	return nil
}

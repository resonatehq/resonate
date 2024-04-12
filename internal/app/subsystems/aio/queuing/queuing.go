package queuing

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/connections"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/connections/t_conn"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/routes"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/routes/t_route"
	"github.com/resonatehq/resonate/internal/util"
)

var (
	ErrConnectionNotFound = errors.New("connection not found")
)

type (
	// Config is the configuration for the queuing subsystem.
	Config struct {
		Connections []*t_conn.ConnectionConfig
		Routes      []*t_route.RoutingConfig
	}

	// QueuingSubsystem is a subsystem that dispatches tasks to user defined connections.
	QueuingSubsystem struct {
		// baseURL is the base URL for the API to use by the workers.
		baseURL string

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
)

// New creates a new queuing subsystem with the given config.
func New(baseURL string, config *Config) (aio.Subsystem, error) {
	var (
		conns      = make(map[string]t_conn.Connection, len(config.Connections))
		connSQ     = make(map[string]chan *t_conn.ConnectionSubmission, len(config.Connections))
		connRouter = NewRouter()
		err        error
	)

	// Create a connection for each connection configuration.
	for _, cfg := range config.Connections {
		tsq := make(chan *t_conn.ConnectionSubmission, 100)        // TODO: make this configurable
		connSQ[cfg.Name] = tsq                                     // for communication between worker and connection
		conns[cfg.Name], err = connections.NewConnection(tsq, cfg) // for starting the connection
		if err != nil {
			return nil, err
		}
	}

	// Register the routes with the desired router (right now only the default pattern router is supported).
	for _, cfg := range config.Routes {

		// Check if target connection exists.
		if _, ok := conns[cfg.Target.Connection]; !ok {
			return nil, fmt.Errorf("validation error for route '%s': %w", cfg.Name, ErrConnectionNotFound)
		}

		route, err := routes.NewRoute(cfg)
		if err != nil {
			return nil, err
		}

		connRouter.Handle(route.Route(), &RouteHandler{ // for routing requests to the appropriate connection
			Connection: cfg.Target.Connection,
			Queue:      cfg.Target.Queue,
		})
		// Workaround for no dependency injection in coroutines. (TODO: consider alternatives)
		CoroutineRouter().Handle(route.Route(), &RouteHandler{
			Connection: cfg.Target.Connection,
			Queue:      cfg.Target.Queue,
		})
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &QueuingSubsystem{
		baseURL:          baseURL,
		connectionRouter: connRouter,
		connections:      conns,
		connectionsSQ:    connSQ,
		connectionsWG:    &sync.WaitGroup{},
		ctx:              ctx,
		stop:             cancel,
	}, nil
}

// String returns the name of the subsystem.
func (t *QueuingSubsystem) String() string {
	return "queuing:default"
}

// NewWorker creates a new worker for the queuing subsystem with the submission queues for each connection.
func (t *QueuingSubsystem) NewWorker(i int) aio.Worker {
	return &QueuingWorker{
		BaseURL:          t.baseURL,
		ConnectionRouter: t.connectionRouter,
		ConnectionsSQ:    t.connectionsSQ,
		i:                i,
	}
}

// Start dispatches all connection to their own goroutines.
func (t *QueuingSubsystem) Start() error {
	t.connectionsWG.Add(len(t.connections))

	for _, bind := range util.OrderedRange(t.connections) {
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

package connections

import (
	"fmt"

	http_conn "github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/connections/http"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/connections/t_conn"
)

func NewConnection(tasks <-chan *t_conn.ConnectionSubmission, cfg *t_conn.ConnectionConfig) (t_conn.Connection, error) {
	// Validate all required fields are present.
	if cfg == nil {
		return nil, fmt.Errorf("connection config is empty")
	}
	if cfg.Name == "" {
		return nil, fmt.Errorf("field 'name' is empty for connection '%s'", cfg.Name)
	}
	if cfg.Kind == "" {
		return nil, fmt.Errorf("field 'kind' is empty for connection '%s'", cfg.Name)
	}

	var (
		conn t_conn.Connection
		err  error
	)

	switch cfg.Kind {
	case t_conn.HTTP:
		conn = http_conn.New()
		err = conn.Init(tasks, cfg.Metadata)
	default:
		return nil, fmt.Errorf("invalid queuing kind: %s", cfg.Kind)
	}

	if err != nil {
		return nil, err
	}

	return conn, nil
}

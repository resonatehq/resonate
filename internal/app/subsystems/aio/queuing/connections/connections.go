package connections

import (
	"fmt"

	http_conn "github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/connections/http"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/connections/t_conn"
)

func NewConnectionOrDie(tasks <-chan *t_conn.ConnectionSubmission, cfg *t_conn.ConnectionConfig) t_conn.Connection {
	var (
		conn t_conn.Connection
		err  error
	)

	switch cfg.Kind {
	case t_conn.HTTP:
		conn = http_conn.New()
		err = conn.Init(tasks, cfg.Metadata)
	default:
		panic(fmt.Sprintf("invalid queuing kind: %s", cfg.Kind))
	}

	if err != nil {
		panic(err)
	}

	return conn
}

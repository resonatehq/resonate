package serve

import (
	"encoding/json"

	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/connections/t_conn"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/routes/t_route"
)

type (
	// ConnectionSlice is a slice of connections a user can define to configure the queueing subsystem.
	ConnectionSlice []*t_conn.ConnectionConfig

	// RouteSlice is a slice of routing configurations a user can define to configure the queueing subsystem.
	RouteSlice []*t_route.RoutingConfig
)

func (c *ConnectionSlice) String() string {
	if c == nil || len(*c) == 0 {
		return ""
	}
	jsonStr, _ := json.Marshal(c)
	return string(jsonStr)
}

func (c *ConnectionSlice) Set(v string) error {
	connections := make([]*t_conn.ConnectionConfig, 0)
	if err := json.Unmarshal([]byte(v), &connections); err != nil {
		return err
	}
	*c = ConnectionSlice(connections)
	return nil
}

func (r *ConnectionSlice) Type() string {
	return "ConnectionSlice"
}

func (r *RouteSlice) String() string {
	if r == nil || len(*r) == 0 {
		return ""
	}
	jsonStr, _ := json.Marshal(r)
	return string(jsonStr)
}

func (r *RouteSlice) Set(v string) error {
	routings := make([]*t_route.RoutingConfig, 0)
	if err := json.Unmarshal([]byte(v), &routings); err != nil {
		return err
	}
	*r = RouteSlice(routings)
	return nil
}

func (r *RouteSlice) Type() string {
	return "RouteSlice"
}

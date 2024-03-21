package serve

import (
	"encoding/json"

	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/connections/t_conn"
)

type (
	// ConnectionSlice is a slice of connections a user can define to configure the queueing subsystem.
	ConnectionSlice []*t_conn.ConnectionConfig
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

func (c *ConnectionSlice) Type() string {
	return "ConnectionSlice"
}

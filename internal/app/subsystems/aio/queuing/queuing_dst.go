package queuing

import (
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/connections/t_conn"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/metadata"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/routes/t_route"
)

// NewDST is a simple helper functions that wraps New and returns a pre-configured QueuingSubsystem.
// This configurations aligns with the DST tests. Search for: 'id = fmt.Sprintf("/gpu/summarize/%s", id)'
// TOOD: make a real DST one that just mocks network calls.
func NewDST() (*QueuingSubsystem, error) {
	queuing, err := New("http://localhost:8001", &Config{
		Connections: []*t_conn.ConnectionConfig{
			{
				Kind: t_conn.HTTP,
				Name: "summarize",
				Metadata: &metadata.Metadata{
					Properties: map[string]interface{}{
						"url": "http://localhost:5001",
					},
				},
			},
		},
		Routes: []*t_route.RoutingConfig{
			{
				Kind: t_route.Pattern,
				Name: "default",
				Target: &t_route.Target{
					Connection: "summarize",
					Queue:      "analytics",
				},
				Metadata: &metadata.Metadata{
					Properties: map[string]interface{}{
						"pattern": "/gpu/summarize/*",
					},
				},
			},
		},
	})
	if err != nil {
		return nil, err
	}

	return queuing, nil
}

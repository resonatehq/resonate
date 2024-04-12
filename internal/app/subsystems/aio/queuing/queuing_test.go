package queuing

import (
	"errors"
	"testing"

	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/connections/t_conn"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/metadata"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/routes/t_route"
)

func TestQueuingSubsystem(t *testing.T) {
	testCases := []struct {
		name          string
		config        *Config
		expectedError error
	}{
		{
			name: "valid config",
			config: &Config{
				Connections: []*t_conn.ConnectionConfig{
					{
						Name: "test_connection",
						Kind: t_conn.HTTP,
						Metadata: &metadata.Metadata{
							Properties: map[string]interface{}{
								"url": "http://example.com",
							},
						},
					},
				},
				Routes: []*t_route.RoutingConfig{
					{
						Name: "test_route",
						Kind: t_route.Pattern,
						Target: &t_route.Target{
							Connection: "test_connection",
							Queue:      "test_queue",
						},
						Metadata: &metadata.Metadata{
							Properties: map[string]interface{}{
								"pattern": "/test/*",
							},
						},
					},
				},
			},
			expectedError: nil,
		},
		{
			name: "missing connection for route",
			config: &Config{
				Connections: []*t_conn.ConnectionConfig{},
				Routes: []*t_route.RoutingConfig{
					{
						Name: "test_route",
						Kind: t_route.Pattern,
						Target: &t_route.Target{
							Connection: "missing_connection",
							Queue:      "test_queue",
						},
						Metadata: &metadata.Metadata{
							Properties: map[string]interface{}{
								"pattern": "/test/*",
							},
						},
					},
				},
			},
			expectedError: ErrConnectionNotFound,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New("", tc.config)
			if tc.expectedError != nil {
				if err == nil {
					t.Errorf("expected error: %s, got nil", tc.expectedError)
				} else if !errors.Is(err, tc.expectedError) {
					t.Errorf("expected error: %s, got: %s", tc.expectedError, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %s", err.Error())
				}
			}
		})
	}
}

func TestQueuingSubsystem_StartStop(t *testing.T) {
	config := &Config{
		Connections: []*t_conn.ConnectionConfig{
			{
				Name: "test_connection",
				Kind: t_conn.HTTP,
				Metadata: &metadata.Metadata{
					Properties: map[string]interface{}{
						"url": "http://example.com",
					},
				},
			},
		},
		Routes: []*t_route.RoutingConfig{},
	}

	qs, err := New("", config)
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	err = qs.Start()
	if err != nil {
		t.Errorf("unexpected error: %s", err.Error())
	}

	err = qs.Stop()
	if err != nil {
		t.Errorf("unexpected error: %s", err.Error())
	}
}

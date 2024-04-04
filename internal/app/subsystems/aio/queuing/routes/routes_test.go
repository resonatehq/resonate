package routes

import (
	"errors"
	"testing"

	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/metadata"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/routes/t_route"
)

func TestNewRoute(t *testing.T) {
	testCases := []struct {
		name          string
		config        *t_route.RoutingConfig
		expectedError error
	}{
		{
			name:          "nil config",
			config:        nil,
			expectedError: ErrMissingRoutingConfig,
		},
		{
			name:          "empty name",
			config:        &t_route.RoutingConfig{},
			expectedError: ErrMissingFieldName,
		},
		{
			name:          "empty kind",
			config:        &t_route.RoutingConfig{Name: "test"},
			expectedError: ErrMissingFieldKind,
		},
		{
			name:          "nil target",
			config:        &t_route.RoutingConfig{Name: "test", Kind: t_route.Pattern},
			expectedError: ErrMissingFieldTarget,
		},
		{
			name: "empty target connection",
			config: &t_route.RoutingConfig{
				Name:   "test",
				Kind:   t_route.Pattern,
				Target: &t_route.Target{},
			},
			expectedError: ErrMissingFieldConn,
		},
		{
			name: "empty target queue",
			config: &t_route.RoutingConfig{
				Name: "test",
				Kind: t_route.Pattern,
				Target: &t_route.Target{
					Connection: "amqp://localhost",
				},
			},
			expectedError: ErrMissingFieldQueue,
		},
		{
			name: "invalid routing kind",
			config: &t_route.RoutingConfig{
				Name: "test",
				Kind: "invalid",
				Target: &t_route.Target{
					Connection: "amqp://localhost",
					Queue:      "test_queue",
				},
			},
			expectedError: ErrInvalidRoutingKind,
		},
		{
			name: "valid config",
			config: &t_route.RoutingConfig{
				Name: "test",
				Kind: t_route.Pattern,
				Target: &t_route.Target{
					Connection: "amqp://localhost",
					Queue:      "test_queue",
				},
				Metadata: &metadata.Metadata{
					Properties: map[string]interface{}{
						"pattern": "/test/*",
					},
				},
			},
			expectedError: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewRoute(tc.config)
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

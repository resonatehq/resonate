package connections

import (
	"errors"
	"testing"

	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/connections/t_conn"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/metadata"
)

func TestNewConnection(t *testing.T) {
	testCases := []struct {
		name          string
		config        *t_conn.ConnectionConfig
		expectedError error
	}{
		{
			name:          "nil config",
			config:        nil,
			expectedError: ErrMissingConnectionConfig,
		},
		{
			name:          "empty name",
			config:        &t_conn.ConnectionConfig{},
			expectedError: ErrMissingFieldName,
		},
		{
			name:          "empty kind",
			config:        &t_conn.ConnectionConfig{Name: "test"},
			expectedError: ErrMissingFieldKind,
		},
		{
			name:          "nil metadata",
			config:        &t_conn.ConnectionConfig{Name: "test", Kind: t_conn.HTTP},
			expectedError: ErrMissingMetadata,
		},
		{
			name: "nil metadata properties",
			config: &t_conn.ConnectionConfig{
				Name:     "test",
				Kind:     t_conn.HTTP,
				Metadata: &metadata.Metadata{},
			},
			expectedError: ErrMissingMetadataProperties,
		},
		{
			name: "invalid connection kind",
			config: &t_conn.ConnectionConfig{
				Name: "test",
				Kind: "invalid",
				Metadata: &metadata.Metadata{
					Properties: map[string]interface{}{},
				},
			},
			expectedError: ErrInvalidConnectionKind,
		},
		{
			name: "valid config",
			config: &t_conn.ConnectionConfig{
				Name: "test",
				Kind: t_conn.HTTP,
				Metadata: &metadata.Metadata{
					Properties: map[string]interface{}{
						"url": "http://example.com",
					},
				},
			},
			expectedError: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tasks := make(chan *t_conn.ConnectionSubmission)
			_, err := NewConnection(tasks, tc.config)
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

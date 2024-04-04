package pattern

import (
	"errors"
	"testing"

	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/metadata"
)

func TestPattern(t *testing.T) {
	tcs := []struct {
		name          string
		meta          *metadata.Metadata
		expectedError error
	}{
		{
			name: "empty pattern",
			meta: &metadata.Metadata{
				Properties: map[string]interface{}{},
			},
			expectedError: ErrMissingPattern,
		},
		{
			name: "missing pattern",
			meta: &metadata.Metadata{
				Properties: map[string]interface{}{
					"pattern": "",
				},
			},
			expectedError: ErrMissingPattern,
		},
		{
			name: "normal",
			meta: &metadata.Metadata{
				Properties: map[string]interface{}{
					"pattern": "/gpu/summarize/*",
				},
			},
			expectedError: nil,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(tc.meta)
			if tc.expectedError != nil {
				if err == nil {
					t.Errorf("expected error: %v, got nil", tc.expectedError)
				} else if !errors.Is(err, tc.expectedError) {
					t.Errorf("expected error: %v, got: %v", tc.expectedError, err)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

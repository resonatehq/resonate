package util

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNext(t *testing.T) {
	testCases := []struct {
		name         string
		curr         int64
		cronExp      string
		expectedNext int64
		expectedErr  error
	}{
		{
			name:         "valid cron (physical clock)",
			curr:         1704719383520,
			cronExp:      "* * * * *",
			expectedNext: 1704719400000,
			expectedErr:  nil,
		},
		{
			name:         "invalid cron (physical clock)",
			curr:         1704719383520,
			cronExp:      "random",
			expectedNext: 0,
			expectedErr:  fmt.Errorf("expected 5 to 6 fields, found 1: [random]"),
		},
		{
			name:         "valid cron (logical clock)",
			curr:         2,
			cronExp:      "2 * * * *",
			expectedNext: 4,
			expectedErr:  nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			next, err := Next(tc.curr, tc.cronExp)

			assert.Equal(t, tc.expectedErr, err)

			if next != tc.expectedNext {
				t.Fatalf("expected next %d and err %v got %d and %v",
					tc.expectedNext, tc.expectedErr, next, err)
			}
		})
	}
}

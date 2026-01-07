package promise

import (
	"testing"
)

func TestGetTimedoutState(t *testing.T) {
	testCases := []struct {
		name     string
		promise  Promise
		expected State
	}{
		{
			name: "Timeout without resonate:timeout tag",
			promise: Promise{
				Tags: map[string]string{},
			},
			expected: Timedout,
		},
		{
			name: "Timeout with resonate:timeout=false tag",
			promise: Promise{
				Tags: map[string]string{
					"resonate:timeout": "false",
				},
			},
			expected: Timedout,
		},
		{
			name: "Resolved with resonate:timeout=true tag",
			promise: Promise{
				Tags: map[string]string{
					"resonate:timeout": "true",
				},
			},
			expected: Resolved,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := GetTimedoutState(tc.promise.Tags)
			if result != tc.expected {
				t.Errorf("Expected %v, got %v", tc.expected, result)
			}
		})
	}
}

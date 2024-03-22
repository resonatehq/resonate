package queuing

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRouting(t *testing.T) {

	tcs := []struct {
		name           string
		pattern        string
		route          string
		expectedResult *MatchResult
		expectedErr    error
	}{
		{
			name:    "simple .com",
			pattern: "/payments/*",
			route:   "http://demo.example.com/payments/123",
			expectedResult: &MatchResult{
				Route:        "/http://demo.example.com/payments/123",
				RoutePattern: "/{http}://{domain}/payments/*",
				Connection:   "conn1",
			},
		},
		{
			name:    "simple .io",
			pattern: "/payments/*",
			route:   "http://demo.example.io/payments/123",
			expectedResult: &MatchResult{
				Route:        "/http://demo.example.io/payments/123",
				RoutePattern: "/{http}://{domain}/payments/*",
				Connection:   "conn1",
			},
		},
		{
			name:    "simple localhost",
			pattern: "/payments/*",
			route:   "http://localhost/payments/123",
			expectedResult: &MatchResult{
				Route:        "/http://localhost/payments/123",
				RoutePattern: "/{http}://{domain}/payments/*",
				Connection:   "conn1",
			},
		},
		{
			name:    "simple localhost https",
			pattern: "/payments/*",
			route:   "https://localhost/payments/123",
			expectedResult: &MatchResult{
				Route:        "/https://localhost/payments/123",
				RoutePattern: "/{http}://{domain}/payments/*",
				Connection:   "conn1",
			},
		},
		{
			name:        "simple .com",
			pattern:     "/payments/*",
			route:       "http://demo.example.com/analytics/123",
			expectedErr: ErrRouteDoesNotMatchAnyPattern,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			router := NewRouter()

			router.Handle(tc.pattern, &RouteHandler{
				Connection: "conn1",
			})

			result, err := router.Match(tc.route)
			if tc.expectedErr != nil {
				assert.Equal(t, tc.expectedErr, err)
				return
			}

			assert.Equal(t, tc.expectedResult.Route, result.Route)
			assert.Equal(t, tc.expectedResult.RoutePattern, result.RoutePattern)
			assert.Equal(t, tc.expectedResult.Connection, result.Connection)
		})

	}
}

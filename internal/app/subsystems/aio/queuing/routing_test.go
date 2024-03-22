package queuing

import (
	"fmt"
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
			name:    "payment simple",
			pattern: "/payments/*",
			route:   "payments/123", // promise id
			expectedResult: &MatchResult{
				Route:        "/payments/123",
				RoutePattern: "/payments/*",
				Connection:   "conn1",
			},
		},
		{
			name:    "payment simple with slash",
			pattern: "/payments/*",
			route:   "/payments/123", // promise id
			expectedResult: &MatchResult{
				Route:        "/payments/123",
				RoutePattern: "/payments/*",
				Connection:   "conn1",
			},
		},
		{
			name:        "absolute url does not work",
			pattern:     "/payments/*",
			route:       "http://demo.example.com/payments/123",
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
			if err != nil {
				assert.Equal(t, tc.expectedErr, err)
				return
			}

			fmt.Printf("result: %+v\n", result)

			assert.Equal(t, tc.expectedResult.Route, result.Route)
			assert.Equal(t, tc.expectedResult.RoutePattern, result.RoutePattern)
			assert.Equal(t, tc.expectedResult.Connection, result.Connection)
		})

	}
}

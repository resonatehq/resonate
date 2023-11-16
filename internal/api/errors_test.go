package api

import (
	"errors"
	"net/http"
	"testing"

	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/stretchr/testify/require"
)

func TestHandleResonateError(t *testing.T) {
	testCases := []struct {
		name           string
		inputError     *t_api.ResonateError
		expectedCode   int
		expectedStatus string
	}{
		{
			name:           "SystemShuttingDown",
			inputError:     t_api.NewResonateError(t_api.ErrSystemShuttingDown, "", errors.New("")),
			expectedCode:   http.StatusServiceUnavailable,
			expectedStatus: "5001",
		},
		{
			name:           "APISubmissionQueueFull",
			inputError:     t_api.NewResonateError(t_api.ErrAPISubmissionQueueFull, "", errors.New("")),
			expectedCode:   http.StatusServiceUnavailable,
			expectedStatus: "5002",
		},
		{
			name:           "AIOSubmissionQueueFull",
			inputError:     t_api.NewResonateError(t_api.ErrAIOSubmissionQueueFull, "", errors.New("")),
			expectedCode:   http.StatusServiceUnavailable,
			expectedStatus: "5003",
		},
		{
			name:           "AIONetworkFailure",
			inputError:     t_api.NewResonateError(t_api.ErrAIONetworkFailure, "", errors.New("")),
			expectedCode:   http.StatusInternalServerError,
			expectedStatus: "5004",
		},
		{
			name:           "AIOStoreFailure",
			inputError:     t_api.NewResonateError(t_api.ErrAIOStoreFailure, "", errors.New("")),
			expectedCode:   http.StatusInternalServerError,
			expectedStatus: "5005",
		},
		{
			name:           "AIOStoreSerializationFailure",
			inputError:     t_api.NewResonateError(t_api.ErrAIOStoreSerializationFailure, "", errors.New("")),
			expectedCode:   http.StatusInternalServerError,
			expectedStatus: "5006",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := HandleResonateError(tc.inputError)

			require.Equal(t, tc.expectedCode, result.APIError.Code)
			require.Equal(t, tc.expectedStatus, result.APIError.Status)
		})
	}
}

func TestHandleRequestError(t *testing.T) {
	testCases := []struct {
		name           string
		inputError     t_api.ResponseStatus
		expectedCode   int
		expectedStatus string
	}{
		{
			name:           "PromiseAlreadyResolved",
			inputError:     t_api.StatusPromiseAlreadyResolved,
			expectedCode:   http.StatusForbidden,
			expectedStatus: "4030",
		},
		{
			name:           "PromiseAlreadyRejected",
			inputError:     t_api.StatusPromiseAlreadyRejected,
			expectedCode:   http.StatusForbidden,
			expectedStatus: "4031",
		},
		{
			name:           "PromiseAlreadyCanceled",
			inputError:     t_api.StatusPromiseAlreadyCanceled,
			expectedCode:   http.StatusForbidden,
			expectedStatus: "4032",
		},
		{
			name:           "PromiseAlreadyTimedout",
			inputError:     t_api.StatusPromiseAlreadyTimedOut,
			expectedCode:   http.StatusForbidden,
			expectedStatus: "4033",
		},
		{
			name:           "PromiseNotFound",
			inputError:     t_api.StatusPromiseNotFound,
			expectedCode:   http.StatusNotFound,
			expectedStatus: "4040",
		},
		{
			name:           "PromiseAlreadyExists",
			inputError:     t_api.StatusPromiseAlreadyExists,
			expectedCode:   http.StatusConflict,
			expectedStatus: "4090",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := HandleRequestError(tc.inputError)

			require.Equal(t, tc.expectedCode, result.APIError.Code)
			require.Equal(t, tc.expectedStatus, result.APIError.Status)
		})
	}
}

package api

import (
	"errors"
	"net/http"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/stretchr/testify/require"
)

func TestHandleResonateError(t *testing.T) {
	testCases := []struct {
		name         string
		inputError   *t_api.ResonateError
		expectedCode int
	}{
		{
			name:         "SystemShuttingDown",
			inputError:   t_api.NewResonateError(t_api.ErrSystemShuttingDown, "", errors.New("")),
			expectedCode: http.StatusServiceUnavailable,
		},
		{
			name:         "APISubmissionQueueFull",
			inputError:   t_api.NewResonateError(t_api.ErrAPISubmissionQueueFull, "", errors.New("")),
			expectedCode: http.StatusServiceUnavailable,
		},
		{
			name:         "AIOSubmissionQueueFull",
			inputError:   t_api.NewResonateError(t_api.ErrAIOSubmissionQueueFull, "", errors.New("")),
			expectedCode: http.StatusServiceUnavailable,
		},
		{
			name:         "AIONetworkFailure",
			inputError:   t_api.NewResonateError(t_api.ErrAIONetworkFailure, "", errors.New("")),
			expectedCode: http.StatusInternalServerError,
		},
		{
			name:         "AIOStoreFailure",
			inputError:   t_api.NewResonateError(t_api.ErrAIOStoreFailure, "", errors.New("")),
			expectedCode: http.StatusInternalServerError,
		},
		{
			name:         "AIOStoreSerializationFailure",
			inputError:   t_api.NewResonateError(t_api.ErrAIOStoreSerializationFailure, "", errors.New("")),
			expectedCode: http.StatusInternalServerError,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := HandleResonateError(tc.inputError)

			require.Equal(t, tc.expectedCode, result.APIError.Code.HTTP())
		})
	}
}

func TestHandleRequestError(t *testing.T) {
	testCases := []struct {
		name         string
		inputError   t_api.ResponseStatus
		expectedCode int
	}{
		{
			name:         "PromiseAlreadyResolved",
			inputError:   t_api.StatusPromiseAlreadyResolved,
			expectedCode: http.StatusForbidden,
		},
		{
			name:         "PromiseAlreadyRejected",
			inputError:   t_api.StatusPromiseAlreadyRejected,
			expectedCode: http.StatusForbidden,
		},
		{
			name:         "PromiseAlreadyCanceled",
			inputError:   t_api.StatusPromiseAlreadyCanceled,
			expectedCode: http.StatusForbidden,
		},
		{
			name:         "PromiseAlreadyTimedout",
			inputError:   t_api.StatusPromiseAlreadyTimedOut,
			expectedCode: http.StatusForbidden,
		},
		{
			name:         "PromiseNotFound",
			inputError:   t_api.StatusPromiseNotFound,
			expectedCode: http.StatusNotFound,
		},
		{
			name:         "PromiseAlreadyExists",
			inputError:   t_api.StatusPromiseAlreadyExists,
			expectedCode: http.StatusConflict,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := HandleRequestError(tc.inputError)

			require.Equal(t, tc.expectedCode, result.APIError.Code.HTTP())
		})
	}
}

func TestIsRequestError(t *testing.T) {
	tcs := []struct {
		name           string
		inputError     t_api.ResponseStatus
		expectedResult bool
	}{
		{
			name:           "StatusOK",
			inputError:     t_api.StatusOK,
			expectedResult: false,
		},
		{
			name:           "StatusCreated",
			inputError:     t_api.StatusCreated,
			expectedResult: false,
		},
		{
			name:           "StatusNoContent",
			inputError:     t_api.StatusNoContent,
			expectedResult: false,
		},
		{
			name:           "StatusPromiseAlreadyResolved",
			inputError:     t_api.StatusPromiseAlreadyResolved,
			expectedResult: true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			result := IsRequestError(tc.inputError)

			require.Equal(t, tc.expectedResult, result)
		})
	}
}

type fieldErrorMock struct {
	validator.FieldError
	field string
	tag   string
	param string
}

func (m fieldErrorMock) Tag() string   { return m.tag }
func (m fieldErrorMock) Param() string { return m.param }
func (m fieldErrorMock) Field() string { return m.field }
func (m fieldErrorMock) Error() string { return "" }

func TestValidationError(t *testing.T) {
	tcs := []struct {
		name           string
		inputErr       error
		expectedCode   int
		expectedErrMsg string
		expectedDetail string
	}{
		{
			name:           "UnknownError",
			inputErr:       errors.New("unknown error"),
			expectedCode:   http.StatusBadRequest,
			expectedErrMsg: "The request is invalid",
			expectedDetail: "unknown error",
		},
		{
			name: "MissingRequiredField",
			inputErr: validator.ValidationErrors{
				fieldErrorMock{
					field: "Timeout",
					tag:   "required",
				},
			},
			expectedCode:   http.StatusBadRequest,
			expectedErrMsg: "The request is invalid",
			expectedDetail: "The field Timeout is required",
		},
		{
			name: "MustBeGreaterThanOrEqualToZero",
			inputErr: validator.ValidationErrors{
				fieldErrorMock{
					field: "Timeout",
					tag:   "gte",
					param: "0",
				},
			},
			expectedCode:   http.StatusBadRequest,
			expectedErrMsg: "The request is invalid",
			expectedDetail: "The field Timeout must be greater than or equal to 0",
		},
		{
			name: "MustBeGreaterThan",
			inputErr: validator.ValidationErrors{
				fieldErrorMock{
					field: "Limit",
					tag:   "gt",
					param: "0",
				},
			},
			expectedCode:   http.StatusBadRequest,
			expectedErrMsg: "The request is invalid",
			expectedDetail: "The field Limit must be greater than 0",
		},
		{
			name: "MustBeLessThanOrEqualTo",
			inputErr: validator.ValidationErrors{
				fieldErrorMock{
					field: "Limit",
					tag:   "lte",
					param: "100",
				},
			},
			expectedCode:   http.StatusBadRequest,
			expectedErrMsg: "The request is invalid",
			expectedDetail: "The field Limit must be less than or equal to 100",
		},
		{
			name: "MustBeOneOf",
			inputErr: validator.ValidationErrors{
				fieldErrorMock{
					field: "State",
					tag:   "oneofcaseinsensitive",
					param: "pending resolved rejected",
				},
			},
			expectedCode:   http.StatusBadRequest,
			expectedErrMsg: "The request is invalid",
			expectedDetail: "The field State must be either pending, resolved, or rejected",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			result := HandleValidationError(tc.inputErr)

			require.Equal(t, tc.expectedCode, result.APIError.Code.HTTP())
			require.Equal(t, tc.expectedErrMsg, result.APIError.Message)
			require.Equal(t, tc.expectedDetail, result.APIError.Details[0].Message)
		})
	}
}

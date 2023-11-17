package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
)

// APIErrorResponse is the actual error response sent to the client.
type APIErrorResponse struct {
	APIError APIError `json:"error,omitempty"`
}

// Error returns the json string representation of this error.
func (e *APIErrorResponse) Error() string {
	errJSON, _ := json.Marshal(e)
	return string(errJSON)
}

// StatusCode returns the HTTP status code for this error.
func (e *APIErrorResponse) StatusCode() int {
	return e.APIError.Code
}

// Error is the error object sent to the client.
type APIError struct {
	// Code is the HTTP status code for this error.
	Code int `json:"code,omitempty"`

	// Message is the error message.
	Message string `json:"message,omitempty"`

	// Status is the internal status code that indicates the error type.
	Status string `json:"status,omitempty"`

	// Details is a list of details about the error.
	Details []ErrorDetail `json:"details,omitempty"`
}

// ErrorDetails is the error details object sent to the client.
type ErrorDetail struct {
	// Type is the specific error type.
	Type string `json:"@type,omitempty"`

	// Message is a human-readable description of the error.
	Message string `json:"message,omitempty"`

	// Domain is the domain of the error.
	Domain string `json:"domain,omitempty"`

	// Metadata is additional metadata about the error.
	Metadata map[string]string `json:"metadata,omitempty"`
}

// HandleResonateError handles platform level errors and returns an APIErrorResponse.
func HandleResonateError(err *t_api.ResonateError) *APIErrorResponse {
	var apiError APIError

	switch err.Code() {

	// 5xx

	case t_api.ErrSystemShuttingDown:
		apiError = APIError{
			Code:    http.StatusServiceUnavailable,
			Message: err.Error(),
			Status:  err.Code().String(),
		}

	case t_api.ErrAPISubmissionQueueFull:
		apiError = APIError{
			Code:    http.StatusServiceUnavailable,
			Message: err.Error(),
			Status:  err.Code().String(),
		}

	case t_api.ErrAIOSubmissionQueueFull:
		apiError = APIError{
			Code:    http.StatusServiceUnavailable,
			Message: err.Error(),
			Status:  err.Code().String(),
		}

	case t_api.ErrAIONetworkFailure:
		apiError = APIError{
			Code:    http.StatusInternalServerError,
			Message: err.Error(),
			Status:  err.Code().String(),
		}

	case t_api.ErrAIOStoreFailure:
		apiError = APIError{
			Code:    http.StatusInternalServerError,
			Message: err.Error(),
			Status:  err.Code().String(),
		}

	case t_api.ErrAIOStoreSerializationFailure:
		apiError = APIError{
			Code:    http.StatusInternalServerError,
			Message: err.Error(),
			Status:  err.Code().String(),
		}

	default:
		panic(fmt.Sprintf("unknown error code %d", err.Code()))
	}

	apiError.Details = append(apiError.Details, ErrorDetail{
		Type:   "ServerError",
		Domain: "server",
		Metadata: map[string]string{
			"url": fmt.Sprintf("https://docs.resonatehq.io/reference/error-codes#%s", apiError.Status),
		},
	})

	originalError := err.Unwrap()
	if originalError != nil {
		apiError.Details[0].Message = originalError.Error()
	}

	return &APIErrorResponse{APIError: apiError}
}

func IsRequestError(status t_api.ResponseStatus) bool {
	switch status {
	case t_api.StatusOK, t_api.StatusCreated, t_api.StatusNoContent:
		return false
	default:
		return true
	}
}

// HandleRequestError handles application level errors and returns an APIErrorResponse.
func HandleRequestError(status t_api.ResponseStatus) *APIErrorResponse {
	var apiError APIError

	switch status {

	// 4xx

	case t_api.StatusPromiseAlreadyResolved:
		apiError = APIError{
			Code:    http.StatusForbidden,
			Message: "The promise has already been resolved",
			Status:  t_api.StatusPromiseAlreadyResolved.String(),
		}

	case t_api.StatusPromiseAlreadyRejected:
		apiError = APIError{
			Code:    http.StatusForbidden,
			Message: "The promise has already been rejected",
			Status:  t_api.StatusPromiseAlreadyRejected.String(),
		}

	case t_api.StatusPromiseAlreadyCanceled:
		apiError = APIError{
			Code:    http.StatusForbidden,
			Message: "The promise has already been canceled",
			Status:  t_api.StatusPromiseAlreadyCanceled.String(),
		}

	case t_api.StatusPromiseAlreadyTimedOut:
		apiError = APIError{
			Code:    http.StatusForbidden,
			Message: "The promise has already timed out",
			Status:  t_api.StatusPromiseAlreadyTimedOut.String(),
		}

	case t_api.StatusPromiseNotFound:
		apiError = APIError{
			Code:    http.StatusNotFound,
			Message: "The specified promise was not found",
			Status:  t_api.StatusPromiseNotFound.String(),
		}

	case t_api.StatusPromiseAlreadyExists:
		apiError = APIError{
			Code:    http.StatusConflict,
			Message: "A promise with this identifier already exists",
			Status:  t_api.StatusPromiseAlreadyExists.String(),
		}

	default:
		panic(fmt.Sprintf("unknown status code %d", status))
	}

	apiError.Details = append(apiError.Details, ErrorDetail{
		Type:    "RequestError",
		Message: "Request errors are not retryable since they are caused by invalid client requests",
		Domain:  "request",
		Metadata: map[string]string{
			"url": fmt.Sprintf("https://docs.resonatehq.io/reference/error-codes#%s", apiError.Status),
		},
	})

	return &APIErrorResponse{APIError: apiError}
}

// HandleValidationError is handled separately from other application level errors since it is more involved.
func HandleValidationError(err error) *APIErrorResponse {
	var apiError APIError
	apiError.Code = http.StatusBadRequest
	apiError.Message = "The request is invalid"
	apiError.Status = t_api.StatusFieldValidationFailure.String()

	var details []ErrorDetail

	for _, err := range parseBindingError(err) {
		details = append(details, ErrorDetail{
			Type:    "FieldValidationError",
			Message: err,
			Domain:  "validation",
			Metadata: map[string]string{
				"url": fmt.Sprintf("https://docs.resonatehq.io/reference/error-codes#%s", apiError.Status),
			},
		})
	}

	apiError.Details = details

	return &APIErrorResponse{APIError: apiError}
}

func parseBindingError(errs ...error) []string {
	var out []string
	for _, err := range errs {
		switch typedErr := err.(type) {
		case validator.ValidationErrors:
			for _, e := range typedErr {
				out = append(out, parseFieldError(e))
			}
		default:
			out = append(out, err.Error())
		}
	}
	return out
}

func parseFieldError(e validator.FieldError) string {
	fieldPrefix := fmt.Sprintf("The field %s", e.Field())
	tag := strings.Split(e.Tag(), "|")[0]

	switch tag {
	case "required":
		return fmt.Sprintf("%s is required", fieldPrefix)
	case "gte":
		param := e.Param()
		return fmt.Sprintf("%s must be greater than or equal to %s", fieldPrefix, param)
	case "gt":
		param := e.Param()
		return fmt.Sprintf("%s must be greater than %s", fieldPrefix, param)
	case "lte":
		param := e.Param()
		return fmt.Sprintf("%s must be less than or equal to %s", fieldPrefix, param)
	case "oneof", "oneofcaseinsensitive":
		param := e.Param()
		paramArr := strings.Split(param, " ")
		paramArr[len(paramArr)-1] = "or " + paramArr[len(paramArr)-1]
		param = strings.Join(paramArr, ", ")
		return fmt.Sprintf("%s must be either %s", fieldPrefix, param)
	default:
		return e.Error()
	}
}

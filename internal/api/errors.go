package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/resonatehq/resonate/internal/kernel/t_api"
)

// APIErrorResponse is the actual error response sent to the client.
type APIErrorResponse struct {
	APIError APIError `json:"error,omitempty"`
}

// NewErrorResponse creates a new ErrorResponse with the given code and message.
func NewAPIErrorResponse(status t_api.ResponseStatus) *APIErrorResponse {
	return handleAPIErrorResponse(status)
}

// Error returns the json string representation of this error.
func (e *APIErrorResponse) Error() string {
	errJson, _ := json.Marshal(e)
	return string(errJson)
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

	// Reason is a short description why it failed.
	Reason string `json:"reason,omitempty"`

	// Domain is the domain of the error.
	Domain string `json:"domain,omitempty"`

	// Metadata is additional metadata about the error.
	Metadata map[string]string `json:"metadata,omitempty"`

	// FieldViolations is a list of field violations.
	FieldViolations []FieldViolation `json:"fieldViolations,omitempty"`
}

// FieldViolation is a JSON:API field violation object.
type FieldViolation struct {
	Field       string `json:"field,omitempty"`
	Description string `json:"description,omitempty"`
}

// TODO: 400 validation already being handled in api layer !!!
func handleAPIErrorResponse(status t_api.ResponseStatus) *APIErrorResponse {
	var apiError APIError

	switch status {

	// 2xx

	case t_api.StatusOK, t_api.StatusCreated, t_api.StatusNoContent:
		return nil

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

	// 5xx

	case t_api.StatusSystemShuttingDown:
		apiError = APIError{
			Code:    http.StatusServiceUnavailable,
			Message: "The system is shutting down",
			Status:  t_api.StatusSystemShuttingDown.String(),
		}

	case t_api.StatusAPISubmissionQueueFull:
		apiError = APIError{
			Code:    http.StatusServiceUnavailable,
			Message: "The API submission queue is full",
			Status:  t_api.StatusAPISubmissionQueueFull.String(),
		}

	case t_api.StatusAIONetworkSubmissionQueueFull:
		apiError = APIError{
			Code:    http.StatusServiceUnavailable,
			Message: "The AIO network submission queue is full",
			Status:  t_api.StatusAIONetworkSubmissionQueueFull.String(),
		}

	case t_api.StatusAIOStoreSubmissionQueueFull:
		apiError = APIError{
			Code:    http.StatusServiceUnavailable,
			Message: "The AIO store submission queue is full",
			Status:  t_api.StatusAIOStoreSubmissionQueueFull.String(),
		}

	default:
		panic(fmt.Sprintf("unknown status code %d", status))
	}

	// add url to error code page -- think about this
	apiError.Details = append(apiError.Details, ErrorDetail{
		Metadata: map[string]string{
			"url": fmt.Sprintf("https://docs.resonatehq.io/reference/error-codes/%d", apiError.Code), // TODO: make this the internal error code
		},
	})

	return &APIErrorResponse{APIError: apiError}
}

func IsApplicationLevelError(status t_api.ResponseStatus) bool {
	switch status {
	case t_api.StatusOK, t_api.StatusCreated, t_api.StatusNoContent:
		return false
	default:
		return true
	}
}

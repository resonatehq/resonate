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

/*
A few ideas in no particular order
1. I wonder if we can pull the (platform) errors out into a separate file,
then we might be able to enumerate them in one place and
specify that the return type on t
he SQ/CQ must be our own custom type
(that implements the Error interface)

2. What do you think about combining the aio network / aio store full error into one and differentiating on a field in the custom error type?


*/

func HandlePlatformLevelError(err error) *APIErrorResponse {
	var apiError APIError

	switch err {
	case t_api.ErrAPISubmissionQueueFull:
		apiError = APIError{
			Code:    http.StatusServiceUnavailable,
			Message: "The API submission queue is full",
			Status:  err.Error(),
		}

	case t_api.ErrAIONetworkSubmissionQueueFull:
		apiError = APIError{
			Code:    http.StatusServiceUnavailable,
			Message: "The AIO network submission queue is full",
			Status:  err.Error(),
		}

	case t_api.ErrAIOStoreSubmissionQueueFull:
		apiError = APIError{
			Code:    http.StatusServiceUnavailable,
			Message: "The AIO store submission queue is full",
			Status:  err.Error(),
		}

	case t_api.ErrSystemShuttingDown:
		apiError = APIError{
			Code:    http.StatusServiceUnavailable,
			Message: "The system is shutting down",
			Status:  err.Error(),
		}

	case t_api.ErrFailedToReadPromise:
		apiError = APIError{
			Code:    http.StatusInternalServerError,
			Message: "Failed to read promise",
			Status:  err.Error(),
		}

	case t_api.ErrFailedToParsePromiseRecord:
		apiError = APIError{
			Code:    http.StatusInternalServerError,
			Message: "Failed to parse promise record",
			Status:  err.Error(),
		}

	case t_api.ErrFailedToTimeoutPromise:
		apiError = APIError{
			Code:    http.StatusInternalServerError,
			Message: "Failed to timeout promise",
			Status:  err.Error(),
		}

	case t_api.ErrFailedToUpdatePromise:
		apiError = APIError{
			Code:    http.StatusInternalServerError,
			Message: "Failed to update promise",
			Status:  err.Error(),
		}

	case t_api.ErrNetworkFailure:
		apiError = APIError{
			Code:    http.StatusInternalServerError,
			Message: "Network failure",
			Status:  err.Error(),
		}

	case t_api.ErrStoreFailure:
		apiError = APIError{
			Code:    http.StatusInternalServerError,
			Message: "Store failure",
			Status:  err.Error(),
		}
	default:
		apiError = APIError{
			Code: http.StatusInternalServerError,
		}
	}

	return &APIErrorResponse{APIError: apiError}
}

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

	default:
		panic(fmt.Sprintf("unknown status code %d", status))
	}

	apiError.Details = append(apiError.Details, ErrorDetail{
		Metadata: map[string]string{
			"url": fmt.Sprintf("https://docs.resonatehq.io/reference/error-codes#%s", apiError.Status),
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
			// FieldViolations: []FieldViolation{
			// 	{
			// 		Field: "param",
			// 	},
			// },
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
	default:
		return e.Error()
	}
}

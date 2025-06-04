package api

import (
	"errors"
	"fmt"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
)

type Error struct {
	// Code is the internal code that indicates the type of error
	Code t_api.StatusCode `json:"code,omitempty"`

	// Message is the error message
	Message string `json:"message,omitempty"`

	// Details is a list of details about the error
	Details []*ErrorDetails `json:"details,omitempty"`
}

type ErrorDetails struct {
	// Type is the specific error type
	Type string `json:"@type,omitempty"`

	// Message is a human readable description of the error
	Message string `json:"message,omitempty"`

	// Domain is the domain of the error
	Domain string `json:"domain,omitempty"`

	// Metadata is additional information about the error
	Metadata map[string]string `json:"metadata,omitempty"`
}

func (e *Error) Error() string {
	return e.Message
}

func ServerError(err error) *Error {
	var error *t_api.Error
	util.Assert(errors.As(err, &error), "must be a resonate error")

	var message string
	if error.Unwrap() != nil {
		message = error.Unwrap().Error()
	}

	return &Error{
		Code:    error.Code(),
		Message: error.Error(),
		Details: []*ErrorDetails{{
			Type:    "ServerError",
			Message: message,
			Domain:  "server",
			Metadata: map[string]string{
				"url": fmt.Sprintf("https://docs.resonatehq.io/operate/errors#%d", error.Code()),
			},
		}},
	}
}

func RequestError(status t_api.StatusCode) *Error {
	return &Error{
		Code:    status,
		Message: status.String(),
		Details: []*ErrorDetails{{
			Type:    "RequestError",
			Message: "Request errors are not retryable since they are caused by invalid client requests",
			Domain:  "request",
			Metadata: map[string]string{
				"url": fmt.Sprintf("https://docs.resonatehq.io/operate/server-errors#%d", status),
			},
		}},
	}
}

func RequestValidationError(err error) *Error {
	code := t_api.StatusFieldValidationError
	details := []*ErrorDetails{}

	for _, err := range parseBindingError(err) {
		details = append(details, &ErrorDetails{
			Type:    "FieldValidationError",
			Message: err,
			Domain:  "request",
			Metadata: map[string]string{
				"url": fmt.Sprintf("https://docs.resonatehq.io/operate/server-errors#%d", code),
			},
		})
	}

	return &Error{
		Code:    code,
		Message: code.String(),
		Details: details,
	}
}

// Helper functions

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
	field := strings.ToLower(e.Field())

	switch e.Tag() {
	case "required":
		return fmt.Sprintf("The field %s is required.", field)
	case "min":
		param := e.Param()
		return fmt.Sprintf("The field %s must be be at least length %s.", field, param)
	case "max":
		param := e.Param()
		return fmt.Sprintf("The field %s must be be at most length %s.", field, param)
	case "gt":
		param := e.Param()
		return fmt.Sprintf("The field %s must be greater than %s.", field, param)
	case "gte":
		param := e.Param()
		return fmt.Sprintf("The field %s must be greater than or equal to %s.", field, param)
	case "lt":
		param := e.Param()
		return fmt.Sprintf("The field %s must be less than %s.", field, param)
	case "lte":
		param := e.Param()
		return fmt.Sprintf("The field %s must be less than or equal to %s.", field, param)
	case "oneof", "oneofcaseinsensitive":
		param := e.Param()
		paramArr := strings.Split(param, " ")
		paramArr[len(paramArr)-1] = "or " + paramArr[len(paramArr)-1]
		param = strings.Join(paramArr, ", ")
		return fmt.Sprintf("The field %s must be one of %s.", field, param)
	default:
		return e.Error()
	}
}

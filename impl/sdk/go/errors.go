package resonate

import (
	"errors"
	"fmt"
)

var (
	ErrSuspended      = errors.New("execution suspended")
	ErrAlreadySettled = errors.New("promise already settled")
	ErrTimeout        = errors.New("timeout")
)

// ServerError carries a status code returned by the server alongside a message.
type ServerError struct {
	Code    int
	Message string
}

func (e *ServerError) Error() string {
	return fmt.Sprintf("server error (code=%d): %s", e.Code, e.Message)
}

// DecodingError signals a JSON/base64/utf8 decode failure.
type DecodingError struct{ Msg string }

func (e *DecodingError) Error() string { return "decoding error: " + e.Msg }

// EncodingError signals a serialization failure on the encode path.
type EncodingError struct{ Msg string }

func (e *EncodingError) Error() string { return "encoding error: " + e.Msg }

// ApplicationError is the typed form of a rejected promise's error payload.
type ApplicationError struct{ Message string }

func (e *ApplicationError) Error() string { return "application error: " + e.Message }

// FunctionNotFoundError signals a missing function in the registry.
type FunctionNotFoundError struct{ Name string }

func (e *FunctionNotFoundError) Error() string { return "function not found: " + e.Name }

// AlreadyRegisteredError signals a duplicate function registration.
type AlreadyRegisteredError struct{ Name string }

func (e *AlreadyRegisteredError) Error() string {
	return fmt.Sprintf("function %q is already registered", e.Name)
}

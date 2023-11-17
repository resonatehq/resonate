package t_api

import (
	"fmt"
	"net/http"
	"strconv"

	grpcApi "github.com/resonatehq/resonate/internal/app/subsystems/api/grpc/api"
	"google.golang.org/grpc/codes"
)

// Application level status (2000-4999)

type ResponseStatus int

func (s ResponseStatus) String() string {
	return strconv.Itoa(int(s))
}

// methods to map to http status code
func (s ResponseStatus) HTTP() int {
	return int(s) / 10
}

// we capture the type of ok status in the response object to have the same dedup info as the http api
func (s ResponseStatus) GRPC_OK() grpcApi.Status {
	switch s {
	case StatusOK:
		return grpcApi.Status(http.StatusOK)
	case StatusCreated:
		return grpcApi.Status(http.StatusCreated)
	default:
		panic(fmt.Sprintf("invalid success status: %d", s))
	}
}

const (
	StatusOK                     ResponseStatus = 2000
	StatusCreated                ResponseStatus = 2010
	StatusNoContent              ResponseStatus = 2040
	StatusFieldValidationFailure ResponseStatus = 4000
	StatusPromiseAlreadyResolved ResponseStatus = 4030
	StatusPromiseAlreadyRejected ResponseStatus = 4031
	StatusPromiseAlreadyCanceled ResponseStatus = 4032
	StatusPromiseAlreadyTimedOut ResponseStatus = 4033
	StatusPromiseNotFound        ResponseStatus = 4040
	StatusSubscriptionNotFound   ResponseStatus = 4041
	StatusPromiseAlreadyExists   ResponseStatus = 4090
)

// Platform level errors (5000-5999)

type ResonateErrorCode int

func (e ResonateErrorCode) String() string {
	return strconv.Itoa(int(e))
}

const (
	// catch call for now
	ErrInternalServer = iota + 5000

	// API
	ErrSystemShuttingDown
	ErrAPISubmissionQueueFull

	// AIO
	ErrAIOSubmissionQueueFull
	ErrAIONetworkFailure
	ErrAIOStoreFailure
	ErrAIOStoreSerializationFailure
)

type ResonateError struct {
	code   ResonateErrorCode
	reason string
	ogErr  error
}

func NewResonateError(code ResonateErrorCode, out string, in error) *ResonateError {
	return &ResonateError{
		code:   code,
		reason: out,
		ogErr:  in,
	}
}

func (e *ResonateError) Error() string {
	return e.reason
}

func (e *ResonateError) Unwrap() error {
	return e.ogErr
}

func (e *ResonateError) Code() ResonateErrorCode {
	return e.code
}

func (e *ResonateError) GRPC() codes.Code {
	switch e.code {
	case ErrInternalServer:
		return codes.Internal
	case ErrSystemShuttingDown:
		return codes.Unavailable
	case ErrAPISubmissionQueueFull:
		return codes.Unavailable
	case ErrAIOSubmissionQueueFull:
		return codes.Unavailable
	case ErrAIONetworkFailure:
		return codes.Unavailable
	case ErrAIOStoreFailure:
		return codes.Unavailable
	case ErrAIOStoreSerializationFailure:
		return codes.Unavailable
	default:
		return codes.Unknown
	}
}

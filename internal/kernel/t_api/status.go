package t_api

import (
	"fmt"

	"github.com/resonatehq/resonate/pkg/promise"
	"google.golang.org/grpc/codes"
)

// Application level status (2000-4999)
const (
	StatusOK                     ResponseStatus = 2000
	StatusCreated                ResponseStatus = 2010
	StatusNoContent              ResponseStatus = 2040
	StatusFieldValidationFailure ResponseStatus = 4000
	StatusPromiseAlreadyResolved ResponseStatus = 4030
	StatusPromiseAlreadyRejected ResponseStatus = 4031
	StatusPromiseAlreadyCanceled ResponseStatus = 4032
	StatusPromiseAlreadyTimedOut ResponseStatus = 4033
	StatusLockAlreadyAcquired    ResponseStatus = 4034

	StatusTaskAlreadyTimedOut  ResponseStatus = 4035
	StatusTaskAlreadyCompleted ResponseStatus = 4036
	StatusTaskWrongCounter     ResponseStatus = 4037

	StatusPromiseNotFound      ResponseStatus = 4040
	StatusSubscriptionNotFound ResponseStatus = 4041
	StatusScheduleNotFound     ResponseStatus = 4042
	StatusLockNotFound         ResponseStatus = 4043
	StatusTaskNotFound         ResponseStatus = 4044

	StatusPromiseAlreadyExists  ResponseStatus = 4090
	StatusScheduleAlreadyExists ResponseStatus = 4091
)

// ResponseStatus is the status code for the response.
type ResponseStatus int

// String returns the string representation of the status code.
func (s ResponseStatus) String() string {
	switch s {
	case StatusOK, StatusCreated, StatusNoContent:
		return "The request was successful"
	case StatusFieldValidationFailure:
		return "The request is invalid"
	case StatusPromiseAlreadyResolved:
		return "The promise has already been resolved"
	case StatusPromiseAlreadyRejected:
		return "The promise has already been rejected"
	case StatusPromiseAlreadyCanceled:
		return "The promise has already been canceled"
	case StatusPromiseAlreadyTimedOut:
		return "The promise has already timed out"
	case StatusPromiseNotFound:
		return "The specified promise was not found"
	case StatusSubscriptionNotFound:
		return "The specified subscription was not found"
	case StatusScheduleNotFound:
		return "The specified schedule was not found"
	case StatusPromiseAlreadyExists:
		return "A promise with this identifier already exists"
	case StatusScheduleAlreadyExists:
		return "A schedule with this identifier already exists"
	case StatusLockAlreadyAcquired:
		return "The lock is already acquired"
	case StatusLockNotFound:
		return "The specified lock was not found"
	default:
		panic(fmt.Sprintf("unknown status code %d", s))
	}
}

// HTTP maps to http status code.
func (s ResponseStatus) HTTP() int {
	return int(s) / 10
}

// GRPC maps to grpc status code.
func (s ResponseStatus) GRPC() codes.Code {
	switch s {
	case StatusOK, StatusCreated, StatusNoContent:
		return codes.OK
	case StatusFieldValidationFailure:
		return codes.InvalidArgument
	case StatusPromiseAlreadyResolved, StatusPromiseAlreadyRejected, StatusPromiseAlreadyCanceled, StatusPromiseAlreadyTimedOut:
		return codes.PermissionDenied
	case StatusPromiseNotFound, StatusSubscriptionNotFound:
		return codes.NotFound
	case StatusPromiseAlreadyExists:
		return codes.AlreadyExists
	case StatusScheduleAlreadyExists:
		return codes.AlreadyExists
	case StatusLockAlreadyAcquired:
		return codes.PermissionDenied
	case StatusLockNotFound:
		return codes.NotFound
	default:
		panic(fmt.Sprintf("invalid status: %d", s))
	}
}

func ForbiddenStatus(state promise.State) ResponseStatus {
	switch state {
	case promise.Resolved:
		return StatusPromiseAlreadyResolved
	case promise.Rejected:
		return StatusPromiseAlreadyRejected
	case promise.Canceled:
		return StatusPromiseAlreadyCanceled
	case promise.Timedout:
		return StatusPromiseAlreadyTimedOut
	default:
		panic(fmt.Sprintf("invalid promise state: %s", state))
	}
}

// Platform level errors (5000-5999)
const (
	ErrInternalServer               ResonateErrorCode = 5000
	ErrAIONetworkFailure            ResonateErrorCode = 5001
	ErrAIOStoreFailure              ResonateErrorCode = 5002
	ErrAIOStoreSerializationFailure ResonateErrorCode = 5003
	ErrSystemShuttingDown           ResonateErrorCode = 5030
	ErrAPISubmissionQueueFull       ResonateErrorCode = 5031
	ErrAIOSubmissionQueueFull       ResonateErrorCode = 5032
	ErrSchedulerQueueFull           ResonateErrorCode = 5033
)

type ResonateErrorCode int

func (e ResonateErrorCode) HTTP() int {
	return int(e) / 10
}

func (e ResonateErrorCode) GRPC() codes.Code {
	switch e {
	case ErrInternalServer:
		return codes.Internal
	case ErrAIONetworkFailure:
		return codes.Internal
	case ErrAIOStoreFailure:
		return codes.Internal
	case ErrAIOStoreSerializationFailure:
		return codes.Internal
	case ErrSystemShuttingDown:
		return codes.Unavailable
	case ErrAPISubmissionQueueFull:
		return codes.Unavailable
	case ErrAIOSubmissionQueueFull:
		return codes.Unavailable
	case ErrSchedulerQueueFull:
		return codes.Unavailable
	default:
		panic(fmt.Sprintf("invalid error code: %d", e))
	}
}

type ResonateError struct {
	code          ResonateErrorCode
	reason        string
	originalError error
}

func NewResonateError(code ResonateErrorCode, out string, in error) *ResonateError {
	return &ResonateError{
		code:          code,
		reason:        out,
		originalError: in,
	}
}

func (e *ResonateError) Error() string {
	return e.reason
}

func (e *ResonateError) Unwrap() error {
	return e.originalError
}

func (e *ResonateError) Code() ResonateErrorCode {
	return e.code
}

func (e *ResonateError) Is(target error) bool {
	_, ok := target.(*ResonateError)
	return ok
}

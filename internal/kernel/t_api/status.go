package t_api

import (
	"fmt"
)

// StatusCode represents the type of response that occurred
type StatusCode int

const (
	// Application level status (2000-4999)
	StatusOK        StatusCode = 2000
	StatusCreated   StatusCode = 2010
	StatusNoContent StatusCode = 2040

	StatusFieldValidationError   StatusCode = 4000
	StatusPromiseAlreadyResolved StatusCode = 4030
	StatusPromiseAlreadyRejected StatusCode = 4031
	StatusPromiseAlreadyCanceled StatusCode = 4032
	StatusPromiseAlreadyTimedout StatusCode = 4033
	StatusLockAlreadyAcquired    StatusCode = 4034
	StatusTaskAlreadyClaimed     StatusCode = 4035
	StatusTaskAlreadyCompleted   StatusCode = 4036
	StatusTaskInvalidCounter     StatusCode = 4037
	StatusTaskInvalidState       StatusCode = 4038
	StatusPromiseNotFound        StatusCode = 4040
	StatusScheduleNotFound       StatusCode = 4041
	StatusLockNotFound           StatusCode = 4042
	StatusTaskNotFound           StatusCode = 4043
	StatusPromiseRecvNotFound    StatusCode = 4044
	StatusPromiseAlreadyExists   StatusCode = 4090
	StatusScheduleAlreadyExists  StatusCode = 4091

	// Platform level status (5000-5999)
	StatusInternalServerError    StatusCode = 5000
	StatusAIOEchoError           StatusCode = 5001
	StatusAIOMatchError          StatusCode = 5002
	StatusAIOQueueError          StatusCode = 5003
	StatusAIOStoreError          StatusCode = 5004
	StatusSystemShuttingDown     StatusCode = 5030
	StatusAPISubmissionQueueFull StatusCode = 5031
	StatusAIOSubmissionQueueFull StatusCode = 5032
	StatusSchedulerQueueFull     StatusCode = 5033
)

// String returns the string representation of the status code.
func (s StatusCode) String() string {
	switch s {
	case StatusOK, StatusCreated, StatusNoContent:
		return "The request was successful"
	case StatusFieldValidationError:
		return "The request is invalid"
	case StatusPromiseAlreadyResolved:
		return "The promise has already been resolved"
	case StatusPromiseAlreadyRejected:
		return "The promise has already been rejected"
	case StatusPromiseAlreadyCanceled:
		return "The promise has already been canceled"
	case StatusPromiseAlreadyTimedout:
		return "The promise has already timedout"
	case StatusLockAlreadyAcquired:
		return "The lock is already acquired"
	case StatusTaskAlreadyClaimed:
		return "The task is already claimed"
	case StatusTaskAlreadyCompleted:
		return "The task is already completed"
	case StatusTaskInvalidCounter:
		return "The task counter is invalid"
	case StatusTaskInvalidState:
		return "The task state is invalid"
	case StatusPromiseNotFound:
		return "The specified promise was not found"
	case StatusScheduleNotFound:
		return "The specified schedule was not found"
	case StatusLockNotFound:
		return "The specified lock was not found"
	case StatusTaskNotFound:
		return "The specified task was not found"
	case StatusPromiseAlreadyExists:
		return "The specified promise already exists"
	case StatusScheduleAlreadyExists:
		return "The specified schedule already exists"
	case StatusInternalServerError:
		return "There was an internal server error"
	case StatusAIOEchoError:
		return "There was an error in the echo subsystem"
	case StatusAIOQueueError:
		return "There was an error in the queue subsystem"
	case StatusAIOStoreError:
		return "There was an error in the store subsystem"
	case StatusSystemShuttingDown:
		return "The system is shutting down"
	case StatusAPISubmissionQueueFull:
		return "The api submission queue is full"
	case StatusAIOSubmissionQueueFull:
		return "The aio submission queue is full"
	case StatusSchedulerQueueFull:
		return "The scheduler queue is full"
	default:
		panic(fmt.Sprintf("unknown status code %d", s))
	}
}

func (s StatusCode) IsSuccessful() bool {
	return s >= 2000 && s < 3000
}

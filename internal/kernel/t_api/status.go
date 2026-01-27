package t_api

import (
	"fmt"
)

// StatusCode represents the type of response that occurred
type StatusCode int

const (
	// Application level status (2000-4999)
	StatusOK        StatusCode = 20000
	StatusCreated   StatusCode = 20100
	StatusNoContent StatusCode = 20400

	StatusFieldValidationError   StatusCode = 40000
	StatusUnauthorized           StatusCode = 40100
	StatusForbidden              StatusCode = 40300
	StatusTaskAlreadyClaimed     StatusCode = 40306
	StatusTaskAlreadyCompleted   StatusCode = 40307
	StatusTaskInvalidCounter     StatusCode = 40308
	StatusTaskInvalidState       StatusCode = 40309
	StatusPromiseNotFound        StatusCode = 40400
	StatusScheduleNotFound       StatusCode = 40401
	StatusTaskNotFound           StatusCode = 40403
	StatusPromiseRecvNotFound    StatusCode = 40404
	StatusTaskNotClaimed         StatusCode = 40901
	StatusTaskInvalidVersion     StatusCode = 40902
	StatusTaskPreconditionFailed StatusCode = 41200

	// Platform level status (50000-59909)
	StatusInternalServerError    StatusCode = 50000
	StatusAIOEchoError           StatusCode = 50001
	StatusAIOMatchError          StatusCode = 50002
	StatusAIOQueueError          StatusCode = 50003
	StatusAIOStoreError          StatusCode = 50004
	StatusSystemShuttingDown     StatusCode = 50300
	StatusAPISubmissionQueueFull StatusCode = 50301
	StatusAIOSubmissionQueueFull StatusCode = 50302
	StatusSchedulerQueueFull     StatusCode = 50303
)

// String returns the string representation of the status code.
func (s StatusCode) String() string {
	switch s {
	case StatusOK, StatusCreated, StatusNoContent:
		return "The request was successful"
	case StatusFieldValidationError:
		return "The request is invalid"
	case StatusUnauthorized:
		return "The request is unauthorized"
	case StatusForbidden:
		return "The request is forbidden"
	case StatusTaskAlreadyClaimed:
		return "The task is already claimed"
	case StatusTaskAlreadyCompleted:
		return "The task is already completed"
	case StatusTaskInvalidCounter:
		return "The task counter is invalid"
	case StatusTaskInvalidState:
		return "The task state is invalid"
	case StatusTaskNotClaimed:
		return "The task state is invalid"
	case StatusTaskInvalidVersion:
		return "The task version is invalid"
	case StatusTaskPreconditionFailed:
		return "The task precondition failed"
	case StatusPromiseNotFound:
		return "The specified promise was not found"
	case StatusScheduleNotFound:
		return "The specified schedule was not found"
	case StatusTaskNotFound:
		return "The specified task was not found"
	case StatusPromiseRecvNotFound:
		return "The specified recv couldn't be found"
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
	return s >= 20000 && s < 30000
}

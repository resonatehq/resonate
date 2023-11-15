// In our system, errors are separated into two categories - platform errors and application errors.
// Platform errors represent failures at the runtime level, such as database connection issues, file I/O failures,
// or network request problems.These are usually transient issues that are recoverable if retried later.
// Application errors indicate errors code specific to our business logic and use cases. This separation allows us
// to handle the two types differently - platform errors may trigger retries with backoff, while application errors
// should report immediately to the users since these failures are not typically recoverable by simply retrying.
//
// In our Go system, platform errors are represented as typical Go `error` values returned from function calls. For example:
//
// dbResult, dbErr := database.Query("SELECT...")
//
// The dbErr would contain platform errors like connection failures. While application errors are returned in
// the response object, while the `error` return is `nil`.
package t_api

import "strconv"

type ResponseStatus int

func (s ResponseStatus) String() string {
	return strconv.Itoa(int(s))
}

const (
	// Application level errors (2000-4999)
	StatusOK                     ResponseStatus = 2000 // map to 200 ok
	StatusCreated                ResponseStatus = 2010 // map to 201 created
	StatusNoContent              ResponseStatus = 2040 // map to 204 no content (delete is special case)
	StatusFieldValidationFailure ResponseStatus = 4000 // map to 400 bad request
	StatusPromiseAlreadyResolved ResponseStatus = 4030 // map to 403 forbidden
	StatusPromiseAlreadyRejected ResponseStatus = 4031 // map to 403 forbidden
	StatusPromiseAlreadyCanceled ResponseStatus = 4032 // map to 403 forbidden
	StatusPromiseAlreadyTimedOut ResponseStatus = 4033 // map to 403 forbidden
	StatusPromiseNotFound        ResponseStatus = 4040 // map to 404 not found
	StatusSubscriptionNotFound   ResponseStatus = 4041 // map to 404 not found
	StatusPromiseAlreadyExists   ResponseStatus = 4090 // map to 409 conflict
)

// Platform level errors (5000-5999)
var (
	ErrInternalServer                *PlatformLevelError = &PlatformLevelError{code: 5000} // map to 500 internal server error (for now, but should be exact and not need this in the future)
	ErrAPISubmissionQueueFull        *PlatformLevelError = &PlatformLevelError{code: 5030} // map to 503 service unavailable
	ErrAIONetworkSubmissionQueueFull *PlatformLevelError = &PlatformLevelError{code: 5031} // map to 503 service unavailable
	ErrAIOStoreSubmissionQueueFull   *PlatformLevelError = &PlatformLevelError{code: 5032} // map to 503 service unavailable
	ErrSystemShuttingDown            *PlatformLevelError = &PlatformLevelError{code: 5033} // map to 503 service unavailable

	// coroutine ones -- slog.Error() logs details
	ErrFailedToReadPromise        *PlatformLevelError = &PlatformLevelError{code: 5001}
	ErrFailedToSearchPromises     *PlatformLevelError = &PlatformLevelError{code: 5002}
	ErrFailedToParsePromiseRecord *PlatformLevelError = &PlatformLevelError{code: 5003}
	ErrFailedToTimeoutPromise     *PlatformLevelError = &PlatformLevelError{code: 5004}
	ErrFailedToUpdatePromise      *PlatformLevelError = &PlatformLevelError{code: 5005}
	ErrNetworkFailure             *PlatformLevelError = &PlatformLevelError{code: 5006}
	ErrStoreFailure               *PlatformLevelError = &PlatformLevelError{code: 5007}
	// Subscription
	ErrFailedToReadSubscriptions       *PlatformLevelError = &PlatformLevelError{code: 5008}
	ErrFailedToCreateSubscription      *PlatformLevelError = &PlatformLevelError{code: 5009}
	ErrFailedToReadSubscription        *PlatformLevelError = &PlatformLevelError{code: 5010}
	ErrFailedToParseSubscriptionRecord *PlatformLevelError = &PlatformLevelError{code: 5011}
	ErrFailedToDeleteSubscription      *PlatformLevelError = &PlatformLevelError{code: 5012}
)

type PlatformLevelError struct {
	code int
}

func (e *PlatformLevelError) Error() string {
	return strconv.Itoa(e.code)
}

func (e *PlatformLevelError) Code() int {
	return e.code
}

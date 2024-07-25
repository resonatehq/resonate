package t_api

import (
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
)

func TestResponseStatusString(t *testing.T) {
	tcs := []struct {
		name   string
		status ResponseStatus
		want   string
	}{
		{
			name:   "StatusOK",
			status: StatusOK,
			want:   "The request was successful",
		},
		{
			name:   "StatusCreated",
			status: StatusCreated,
			want:   "The request was successful",
		},
		{
			name:   "StatusNoContent",
			status: StatusNoContent,
			want:   "The request was successful",
		},
		{
			name:   "StatusFieldValidationFailure",
			status: StatusFieldValidationFailure,
			want:   "The request is invalid",
		},
		{
			name:   "StatusPromiseAlreadyResolved",
			status: StatusPromiseAlreadyResolved,
			want:   "The promise has already been resolved",
		},
		{
			name:   "StatusPromiseAlreadyRejected",
			status: StatusPromiseAlreadyRejected,
			want:   "The promise has already been rejected",
		},
		{
			name:   "StatusPromiseAlreadyCanceled",
			status: StatusPromiseAlreadyCanceled,
			want:   "The promise has already been canceled",
		},
		{
			name:   "StatusPromiseAlreadyTimedOut",
			status: StatusPromiseAlreadyTimedout,
			want:   "The promise has already timed out",
		},
		{
			name:   "StatusPromiseNotFound",
			status: StatusPromiseNotFound,
			want:   "The specified promise was not found",
		},
		{
			name:   "StatusSubscriptionNotFound",
			status: StatusSubscriptionNotFound,
			want:   "The specified subscription was not found",
		},
		{
			name:   "StatusPromiseAlreadyExists",
			status: StatusPromiseAlreadyExists,
			want:   "A promise with this identifier already exists",
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.status.String())
		})
	}
}

func TestResponseStatusHTTP(t *testing.T) {
	tcs := []struct {
		name   string
		status ResponseStatus
		want   int
	}{
		{
			name:   "StatusOK",
			status: StatusOK,
			want:   200,
		},
		{
			name:   "StatusCreated",
			status: StatusCreated,
			want:   201,
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.status.HTTP())
		})
	}
}

func TestResponseStatusGRPC(t *testing.T) {
	tcs := []struct {
		name   string
		status ResponseStatus
		want   codes.Code
	}{
		{
			name:   "StatusOK",
			status: StatusOK,
			want:   codes.OK,
		},
		{
			name:   "StatusCreated",
			status: StatusCreated,
			want:   codes.OK,
		},
		{
			name:   "StatusNoContent",
			status: StatusNoContent,
			want:   codes.OK,
		},
		{
			name:   "StatusFieldValidationFailure",
			status: StatusFieldValidationFailure,
			want:   codes.InvalidArgument,
		},
		{
			name:   "StatusPromiseAlreadyResolved",
			status: StatusPromiseAlreadyResolved,
			want:   codes.PermissionDenied,
		},
		{
			name:   "StatusPromiseAlreadyRejected",
			status: StatusPromiseAlreadyRejected,
			want:   codes.PermissionDenied,
		},
		{
			name:   "StatusPromiseAlreadyCanceled",
			status: StatusPromiseAlreadyCanceled,
			want:   codes.PermissionDenied,
		},
		{
			name:   "StatusPromiseAlreadyTimedOut",
			status: StatusPromiseAlreadyTimedout,
			want:   codes.PermissionDenied,
		},
		{
			name:   "StatusPromiseNotFound",
			status: StatusPromiseNotFound,
			want:   codes.NotFound,
		},
		{
			name:   "StatusSubscriptionNotFound",
			status: StatusSubscriptionNotFound,
			want:   codes.NotFound,
		},
		{
			name:   "StatusPromiseAlreadyExists",
			status: StatusPromiseAlreadyExists,
			want:   codes.AlreadyExists,
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.status.GRPC())
		})
	}
}

func TestResonateErrorHTTP(t *testing.T) {
	tcs := []struct {
		name string
		code ResonateErrorCode
		want int
	}{
		{
			name: "ErrSystemShuttingDown",
			code: ErrSystemShuttingDown,
			want: http.StatusServiceUnavailable,
		},
		{
			name: "ErrSchedulerQueueFull",
			code: ErrSchedulerQueueFull,
			want: http.StatusServiceUnavailable,
		},
		{
			name: "ErrAPISubmissionQueueFull",
			code: ErrAPISubmissionQueueFull,
			want: http.StatusServiceUnavailable,
		},
		{
			name: "ErrAIOSubmissionQueueFull",
			code: ErrAIOSubmissionQueueFull,
			want: http.StatusServiceUnavailable,
		},
		{
			name: "ErrAIONetworkFailure",
			code: ErrAIONetworkFailure,
			want: http.StatusInternalServerError,
		},
		{
			name: "ErrAIOStoreFailure",
			code: ErrAIOStoreFailure,
			want: http.StatusInternalServerError,
		},
		{
			name: "ErrAIOStoreSerializationFailure",
			code: ErrAIOStoreSerializationFailure,
			want: http.StatusInternalServerError,
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, NewResonateError(tc.code, "", nil).code.HTTP())
		})
	}
}
func TestResonateErrorGRPC(t *testing.T) {
	tcs := []struct {
		name string
		code ResonateErrorCode
		want codes.Code
	}{
		{
			name: "ErrInternalServer",
			code: ErrInternalServer,
			want: codes.Internal,
		},
		{
			name: "ErrSystemShuttingDown",
			code: ErrSystemShuttingDown,
			want: codes.Unavailable,
		},
		{
			name: "ErrSchedulerQueueFull",
			code: ErrSchedulerQueueFull,
			want: codes.Unavailable,
		},
		{
			name: "ErrAPISubmissionQueueFull",
			code: ErrAPISubmissionQueueFull,
			want: codes.Unavailable,
		},
		{
			name: "ErrAIOSubmissionQueueFull",
			code: ErrAIOSubmissionQueueFull,
			want: codes.Unavailable,
		},
		{
			name: "ErrAIONetworkFailure",
			code: ErrAIONetworkFailure,
			want: codes.Internal,
		},
		{
			name: "ErrAIOStoreFailure",
			code: ErrAIOStoreFailure,
			want: codes.Internal,
		},
		{
			name: "ErrAIOStoreSerializationFailure",
			code: ErrAIOStoreSerializationFailure,
			want: codes.Internal,
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, NewResonateError(tc.code, "", nil).code.GRPC())
		})
	}
}

func TestResonateErrorError(t *testing.T) {
	err := NewResonateError(ErrInternalServer, "internal server error", nil)
	assert.Equal(t, "internal server error", err.Error())
}

func TestResonateErrorUnwrap(t *testing.T) {
	inner := errors.New("inner error")
	err := NewResonateError(ErrInternalServer, "wrapped error", inner)
	assert.Equal(t, inner, err.Unwrap())
}

func TestResonateErrorCode(t *testing.T) {
	err := NewResonateError(ErrInternalServer, "code test", nil)
	assert.Equal(t, ErrInternalServer, err.Code())
}

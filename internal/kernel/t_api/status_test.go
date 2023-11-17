package t_api

import (
	"errors"
	"net/http"
	"testing"

	grpcApi "github.com/resonatehq/resonate/internal/app/subsystems/api/grpc/api"
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
			want:   "2000",
		},
		{
			name:   "StatusCreated",
			status: StatusCreated,
			want:   "2010",
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

func TestResponseStatusGRPC_OK(t *testing.T) {
	tcs := []struct {
		name   string
		status ResponseStatus
		want   grpcApi.Status
	}{
		{
			name:   "StatusOK",
			status: StatusOK,
			want:   grpcApi.Status(http.StatusOK),
		},
		{
			name:   "StatusCreated",
			status: StatusCreated,
			want:   grpcApi.Status(http.StatusCreated),
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.status.GRPC_OK())
		})
	}
}

func TestResonateErrorCode_String(t *testing.T) {
	tcs := []struct {
		name string
		code ResonateErrorCode
		want string
	}{
		{
			name: "ErrInternalServer",
			code: ErrInternalServer,
			want: "5000",
		},
		{
			name: "ErrSystemShuttingDown",
			code: ErrSystemShuttingDown,
			want: "5001",
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.code.String())
		})
	}
}

func TestResonateError_GRPC(t *testing.T) {
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
			want: codes.Unavailable,
		},
		{
			name: "ErrAIOStoreFailure",
			code: ErrAIOStoreFailure,
			want: codes.Unavailable,
		},
		{
			name: "ErrAIOStoreSerializationFailure",
			code: ErrAIOStoreSerializationFailure,
			want: codes.Unavailable,
		},
		{
			name: "ErrUnknown",
			code: 10000,
			want: codes.Unknown,
		},
	}
	for _, tc := range tcs {
		t.Run(tc.want.String(), func(t *testing.T) {
			assert.Equal(t, tc.want, NewResonateError(tc.code, "", nil).GRPC())
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

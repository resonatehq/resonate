// update proto
package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/resonatehq/resonate/internal/api"
	grpcApi "github.com/resonatehq/resonate/internal/app/subsystems/api/grpc/api"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/test"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/idempotency"
	"github.com/resonatehq/resonate/pkg/lock"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/schedule"

	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type grpcTest struct {
	*test.API
	subsystem      api.Subsystem
	errors         chan error
	conn           *grpc.ClientConn
	client         grpcApi.PromisesClient
	scheduleClient grpcApi.SchedulesClient
	lockClient     grpcApi.LocksClient
}

func setup() (*grpcTest, error) {
	api := &test.API{}
	errors := make(chan error)
	subsystem := New(api, &Config{
		Addr: "127.0.0.1:5555",
	})

	// start grpc server
	go subsystem.Start(errors)
	time.Sleep(100 * time.Millisecond)

	conn, err := grpc.Dial("127.0.0.1:5555", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return &grpcTest{
		API:            api,
		subsystem:      subsystem,
		errors:         errors,
		conn:           conn,
		client:         grpcApi.NewPromisesClient(conn),
		scheduleClient: grpcApi.NewSchedulesClient(conn),
		lockClient:     grpcApi.NewLocksClient(conn),
	}, nil
}

func (t *grpcTest) teardown() error {
	defer close(t.errors)

	if err := t.conn.Close(); err != nil {
		return err
	}

	return t.subsystem.Stop()
}

// LOCK

// todo: test here

// PROMISE

func TestReadPromise(t *testing.T) {
	grpcTest, err := setup()
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name    string
		grpcReq *grpcApi.ReadPromiseRequest
		req     *t_api.Request
		res     *t_api.Response
		code    codes.Code // grpc error code
	}{
		{
			name: "ReadPromise",
			grpcReq: &grpcApi.ReadPromiseRequest{
				Id: "foo",
			},
			req: &t_api.Request{
				Kind: t_api.ReadPromise,
				ReadPromise: &t_api.ReadPromiseRequest{
					Id: "foo",
				},
			},
			res: &t_api.Response{
				Kind: t_api.ReadPromise,
				ReadPromise: &t_api.ReadPromiseResponse{
					Status: t_api.StatusOK,
					Promise: &promise.Promise{
						Id:    "foo",
						State: promise.Pending,
					},
				},
			},
			code: codes.OK,
		},
		{
			name: "ReadPromiseNotFound",
			grpcReq: &grpcApi.ReadPromiseRequest{
				Id: "bar",
			},
			req: &t_api.Request{
				Kind: t_api.ReadPromise,
				ReadPromise: &t_api.ReadPromiseRequest{
					Id: "bar",
				},
			},
			res: &t_api.Response{
				Kind: t_api.ReadPromise,
				ReadPromise: &t_api.ReadPromiseResponse{
					Status:  t_api.StatusPromiseNotFound,
					Promise: nil,
				},
			},
			code: codes.NotFound,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			grpcTest.Load(t, tc.req, tc.res)

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			_, err := grpcTest.client.ReadPromise(ctx, tc.grpcReq)
			if err != nil {
				// actual grpc error
				s, ok := status.FromError(err)
				if !ok {
					t.Fatal(err)
				}
				assert.Equal(t, tc.code, s.Code())
				return
			}

			assert.Equal(t, tc.code, codes.OK)

			select {
			case err := <-grpcTest.errors:
				t.Fatal(err)
			default:
			}
		})
	}

	if err := grpcTest.teardown(); err != nil {
		t.Fatal(err)
	}
}

func TestSearchPromises(t *testing.T) {
	grpcTest, err := setup()
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name    string
		grpcReq *grpcApi.SearchPromisesRequest
		req     *t_api.Request
		res     *t_api.Response
	}{
		{
			name: "SearchPromises",
			grpcReq: &grpcApi.SearchPromisesRequest{
				Id:    "*",
				Limit: 10,
			},
			req: &t_api.Request{
				Kind: t_api.SearchPromises,
				SearchPromises: &t_api.SearchPromisesRequest{
					Id: "*",
					States: []promise.State{
						promise.Pending,
						promise.Resolved,
						promise.Rejected,
						promise.Timedout,
						promise.Canceled,
					},
					Limit: 10,
				},
			},
			res: &t_api.Response{
				Kind: t_api.SearchPromises,
				SearchPromises: &t_api.SearchPromisesResponse{
					Status:   t_api.StatusOK,
					Cursor:   nil,
					Promises: []*promise.Promise{},
				},
			},
		},
		{
			name: "SearchPromisesCursor",
			grpcReq: &grpcApi.SearchPromisesRequest{
				Cursor: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJOZXh0Ijp7ImlkIjoiKiIsInN0YXRlcyI6WyJQRU5ESU5HIl0sInRhZ3MiOnt9LCJsaW1pdCI6MTAsInNvcnRJZCI6MTAwfX0.XKusWO-Jl4v7QVIwh5Pn3oIElBvtpf0VPOLJkXPvQLk",
			},
			req: &t_api.Request{
				Kind: t_api.SearchPromises,
				SearchPromises: &t_api.SearchPromisesRequest{
					Id: "*",
					States: []promise.State{
						promise.Pending,
					},
					Tags:   map[string]string{},
					Limit:  10,
					SortId: util.ToPointer(int64(100)),
				},
			},
			res: &t_api.Response{
				Kind: t_api.SearchPromises,
				SearchPromises: &t_api.SearchPromisesResponse{
					Status:   t_api.StatusOK,
					Cursor:   nil, // not checked
					Promises: []*promise.Promise{},
				},
			},
		},
		{
			name: "SearchPromisesPending",
			grpcReq: &grpcApi.SearchPromisesRequest{
				Id:    "*",
				State: grpcApi.SearchState_SEARCH_PENDING,
				Limit: 10,
			},
			req: &t_api.Request{
				Kind: t_api.SearchPromises,
				SearchPromises: &t_api.SearchPromisesRequest{
					Id: "*",
					States: []promise.State{
						promise.Pending,
					},
					Limit: 10,
				},
			},
			res: &t_api.Response{
				Kind: t_api.SearchPromises,
				SearchPromises: &t_api.SearchPromisesResponse{
					Status:   t_api.StatusOK,
					Cursor:   nil,
					Promises: []*promise.Promise{},
				},
			},
		},
		{
			name: "SearchPromisesResolved",
			grpcReq: &grpcApi.SearchPromisesRequest{
				Id:    "*",
				State: grpcApi.SearchState_SEARCH_RESOLVED,
				Limit: 10,
			},
			req: &t_api.Request{
				Kind: t_api.SearchPromises,
				SearchPromises: &t_api.SearchPromisesRequest{
					Id: "*",
					States: []promise.State{
						promise.Resolved,
					},
					Limit: 10,
				},
			},
			res: &t_api.Response{
				Kind: t_api.SearchPromises,
				SearchPromises: &t_api.SearchPromisesResponse{
					Status:   t_api.StatusOK,
					Cursor:   nil,
					Promises: []*promise.Promise{},
				},
			},
		},
		{
			name: "SearchPromisesRejected",
			grpcReq: &grpcApi.SearchPromisesRequest{
				Id:    "*",
				State: grpcApi.SearchState_SEARCH_REJECTED,
				Limit: 10,
			},
			req: &t_api.Request{
				Kind: t_api.SearchPromises,
				SearchPromises: &t_api.SearchPromisesRequest{
					Id: "*",
					States: []promise.State{
						promise.Rejected,
						promise.Timedout,
						promise.Canceled,
					},
					Limit: 10,
				},
			},
			res: &t_api.Response{
				Kind: t_api.SearchPromises,
				SearchPromises: &t_api.SearchPromisesResponse{
					Status:   t_api.StatusOK,
					Cursor:   nil,
					Promises: []*promise.Promise{},
				},
			},
		},
		{
			name: "SearchPromisesTags",
			grpcReq: &grpcApi.SearchPromisesRequest{
				Id: "*",
				Tags: map[string]string{
					"resonate:invocation": "true",
				},
				Limit: 10,
			},
			req: &t_api.Request{
				Kind: t_api.SearchPromises,
				SearchPromises: &t_api.SearchPromisesRequest{
					Id: "*",
					States: []promise.State{
						promise.Pending,
						promise.Resolved,
						promise.Rejected,
						promise.Timedout,
						promise.Canceled,
					},
					Tags: map[string]string{
						"resonate:invocation": "true",
					},
					Limit: 10,
				},
			},
			res: &t_api.Response{
				Kind: t_api.SearchPromises,
				SearchPromises: &t_api.SearchPromisesResponse{
					Status:   t_api.StatusOK,
					Cursor:   nil,
					Promises: []*promise.Promise{},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			grpcTest.Load(t, tc.req, tc.res)

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			_, err := grpcTest.client.SearchPromises(ctx, tc.grpcReq)
			if err != nil {
				t.Fatal(err)
			}

			select {
			case err := <-grpcTest.errors:
				t.Fatal(err)
			default:
			}
		})
	}

	if err := grpcTest.teardown(); err != nil {
		t.Fatal(err)
	}
}

func TestCreatePromise(t *testing.T) {
	grpcTest, err := setup()
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name        string
		grpcReq     *grpcApi.CreatePromiseRequest
		req         *t_api.Request
		res         *t_api.Response
		noop        bool
		expectedErr codes.Code
	}{
		{
			name: "CreatePromise",
			grpcReq: &grpcApi.CreatePromiseRequest{
				Id:             "foo",
				IdempotencyKey: "bar",
				Strict:         true,
				Param: &grpcApi.Value{
					Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
					Data:    []byte("pending"),
				},
				Timeout: 1,
			},
			req: &t_api.Request{
				Kind: t_api.CreatePromise,
				CreatePromise: &t_api.CreatePromiseRequest{
					Id:             "foo",
					IdempotencyKey: util.ToPointer(idempotency.Key("bar")),
					Strict:         true,
					Param: promise.Value{
						Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
						Data:    []byte("pending"),
					},
					Timeout: 1,
				},
			},
			res: &t_api.Response{
				Kind: t_api.CreatePromise,
				CreatePromise: &t_api.CreatePromiseResponse{
					Status: t_api.StatusCreated,
					Promise: &promise.Promise{
						Id:    "foo",
						State: promise.Pending,
					},
				},
			},
			noop: false,
		},
		{
			name: "CreatePromiseMinimal",
			grpcReq: &grpcApi.CreatePromiseRequest{
				Id:      "foo",
				Timeout: 1,
			},
			req: &t_api.Request{
				Kind: t_api.CreatePromise,
				CreatePromise: &t_api.CreatePromiseRequest{
					Id:             "foo",
					IdempotencyKey: nil,
					Strict:         false,
					Param: promise.Value{
						Headers: nil,
						Data:    nil,
					},
					Timeout: 1,
				},
			},
			res: &t_api.Response{
				Kind: t_api.CreatePromise,
				CreatePromise: &t_api.CreatePromiseResponse{
					Status: t_api.StatusCreated,
					Promise: &promise.Promise{
						Id:    "foo",
						State: promise.Pending,
					},
				},
			},
			noop: false,
		},
		{
			name: "CreatePromiseTimeOutRequired",
			grpcReq: &grpcApi.CreatePromiseRequest{
				Id: "foo",
				// Timeout: 1,
			},
			req: &t_api.Request{
				Kind: t_api.CreatePromise,
				CreatePromise: &t_api.CreatePromiseRequest{
					Id:             "foo",
					IdempotencyKey: nil,
					Strict:         false,
					Param: promise.Value{
						Headers: nil,
						Data:    nil,
					},
					Timeout: 1,
				},
			},
			res: &t_api.Response{
				Kind: t_api.CreatePromise,
				CreatePromise: &t_api.CreatePromiseResponse{
					Status: t_api.StatusCreated,
					Promise: &promise.Promise{
						Id:    "foo",
						State: promise.Pending,
					},
				},
			},
			noop:        false,
			expectedErr: codes.InvalidArgument,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			grpcTest.Load(t, tc.req, tc.res)

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			res, err := grpcTest.client.CreatePromise(ctx, tc.grpcReq)
			if err != nil {
				s, ok := status.FromError(err)
				if !ok {
					t.Fatal(err)
				}
				assert.Equal(t, tc.expectedErr, s.Code())
				return
			}

			assert.Equal(t, tc.noop, res.Noop)

			select {
			case err := <-grpcTest.errors:
				t.Fatal(err)
			default:
			}
		})
	}

	if err := grpcTest.teardown(); err != nil {
		t.Fatal(err)
	}
}

func TestCancelPromise(t *testing.T) {
	grpcTest, err := setup()
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name        string
		grpcReq     *grpcApi.CancelPromiseRequest
		req         *t_api.Request
		res         *t_api.Response
		noop        bool
		expectedErr codes.Code
	}{
		{
			name: "CancelPromise",
			grpcReq: &grpcApi.CancelPromiseRequest{
				Id:             "foo",
				IdempotencyKey: "bar",
				Strict:         true,
				Value: &grpcApi.Value{
					Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
					Data:    []byte("cancel"),
				},
			},
			req: &t_api.Request{
				Kind: t_api.CancelPromise,
				CompletePromise: &t_api.CompletePromiseRequest{
					Id:             "foo",
					IdempotencyKey: util.ToPointer(idempotency.Key("bar")),
					Strict:         true,
					State:          promise.Canceled,
					Value: promise.Value{
						Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
						Data:    []byte("cancel"),
					},
				},
			},
			res: &t_api.Response{
				Kind: t_api.CancelPromise,
				CompletePromise: &t_api.CompletePromiseResponse{
					Status: t_api.StatusCreated,
					Promise: &promise.Promise{
						Id:    "foo",
						State: promise.Canceled,
					},
				},
			},
			noop: false,
		},
		{
			name: "CancelPromiseMinimal",
			grpcReq: &grpcApi.CancelPromiseRequest{
				Id: "foo",
			},
			req: &t_api.Request{
				Kind: t_api.CancelPromise,
				CompletePromise: &t_api.CompletePromiseRequest{
					Id:             "foo",
					IdempotencyKey: nil,
					Strict:         false,
					State:          promise.Canceled,
					Value: promise.Value{
						Headers: nil,
						Data:    nil,
					},
				},
			},
			res: &t_api.Response{
				Kind: t_api.CancelPromise,
				CompletePromise: &t_api.CompletePromiseResponse{
					Status: t_api.StatusCreated,
					Promise: &promise.Promise{
						Id:    "foo",
						State: promise.Canceled,
					},
				},
			},
			noop: false,
		},
		{
			name: "CancelPromiseAlreadyResolved",
			grpcReq: &grpcApi.CancelPromiseRequest{
				Id: "foo",
			},
			req: &t_api.Request{
				Kind: t_api.CancelPromise,
				CompletePromise: &t_api.CompletePromiseRequest{
					Id:             "foo",
					IdempotencyKey: nil,
					Strict:         false,
					State:          promise.Canceled,
					Value: promise.Value{
						Headers: nil,
						Data:    nil,
					},
				},
			},
			res: &t_api.Response{
				Kind: t_api.CancelPromise,
				CompletePromise: &t_api.CompletePromiseResponse{
					Status: t_api.StatusPromiseAlreadyResolved,
					Promise: &promise.Promise{
						Id:    "foo",
						State: promise.Resolved,
					},
				},
			},
			noop:        false,
			expectedErr: codes.PermissionDenied,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			grpcTest.Load(t, tc.req, tc.res)

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			res, err := grpcTest.client.CancelPromise(ctx, tc.grpcReq)
			if err != nil {
				s, ok := status.FromError(err)
				if !ok {
					t.Fatal(err)
				}
				assert.Equal(t, tc.expectedErr, s.Code())
				return
			}

			assert.Equal(t, tc.noop, res.Noop)

			select {
			case err := <-grpcTest.errors:
				t.Fatal(err)
			default:
			}
		})
	}

	if err := grpcTest.teardown(); err != nil {
		t.Fatal(err)
	}
}

func TestResolvePromise(t *testing.T) {
	grpcTest, err := setup()
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name        string
		grpcReq     *grpcApi.ResolvePromiseRequest
		req         *t_api.Request
		res         *t_api.Response
		noop        bool
		expectedErr codes.Code
	}{
		{
			name: "ResolvePromise",
			grpcReq: &grpcApi.ResolvePromiseRequest{
				Id:             "foo",
				IdempotencyKey: "bar",
				Strict:         true,
				Value: &grpcApi.Value{
					Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
					Data:    []byte("cancel"),
				},
			},
			req: &t_api.Request{
				Kind: t_api.ResolvePromise,
				CompletePromise: &t_api.CompletePromiseRequest{
					Id:             "foo",
					IdempotencyKey: util.ToPointer(idempotency.Key("bar")),
					Strict:         true,
					State:          promise.Resolved,
					Value: promise.Value{
						Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
						Data:    []byte("cancel"),
					},
				},
			},
			res: &t_api.Response{
				Kind: t_api.ResolvePromise,
				CompletePromise: &t_api.CompletePromiseResponse{
					Status: t_api.StatusCreated,
					Promise: &promise.Promise{
						Id:    "foo",
						State: promise.Resolved,
					},
				},
			},
			noop: false,
		},
		{
			name: "ResolvePromiseMinimal",
			grpcReq: &grpcApi.ResolvePromiseRequest{
				Id: "foo",
			},
			req: &t_api.Request{
				Kind: t_api.ResolvePromise,
				CompletePromise: &t_api.CompletePromiseRequest{
					Id:             "foo",
					IdempotencyKey: nil,
					Strict:         false,
					State:          promise.Resolved,
					Value: promise.Value{
						Headers: nil,
						Data:    nil,
					},
				},
			},
			res: &t_api.Response{
				Kind: t_api.ResolvePromise,
				CompletePromise: &t_api.CompletePromiseResponse{
					Status: t_api.StatusCreated,
					Promise: &promise.Promise{
						Id:    "foo",
						State: promise.Resolved,
					},
				},
			},
			noop: false,
		},
		{
			name: "ResolvePromiseAlreadyRejected",
			grpcReq: &grpcApi.ResolvePromiseRequest{
				Id: "foo",
			},
			req: &t_api.Request{
				Kind: t_api.ResolvePromise,
				CompletePromise: &t_api.CompletePromiseRequest{
					Id:             "foo",
					IdempotencyKey: nil,
					Strict:         false,
					State:          promise.Resolved,
					Value: promise.Value{
						Headers: nil,
						Data:    nil,
					},
				},
			},
			res: &t_api.Response{
				Kind: t_api.ResolvePromise,
				CompletePromise: &t_api.CompletePromiseResponse{
					Status: t_api.StatusPromiseAlreadyRejected,
					Promise: &promise.Promise{
						Id:    "foo",
						State: promise.Rejected,
					},
				},
			},
			noop:        false,
			expectedErr: codes.PermissionDenied,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			grpcTest.Load(t, tc.req, tc.res)

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			res, err := grpcTest.client.ResolvePromise(ctx, tc.grpcReq)
			if err != nil {
				s, ok := status.FromError(err)
				if !ok {
					t.Fatal(err)
				}
				assert.Equal(t, tc.expectedErr, s.Code())
				return
			}

			assert.Equal(t, tc.noop, res.Noop)

			select {
			case err := <-grpcTest.errors:
				t.Fatal(err)
			default:
			}
		})
	}

	if err := grpcTest.teardown(); err != nil {
		t.Fatal(err)
	}
}

func TestRejectPromise(t *testing.T) {
	grpcTest, err := setup()
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name        string
		grpcReq     *grpcApi.RejectPromiseRequest
		req         *t_api.Request
		res         *t_api.Response
		noop        bool
		expectedErr codes.Code
	}{
		{
			name: "RejectPromise",
			grpcReq: &grpcApi.RejectPromiseRequest{
				Id:             "foo",
				IdempotencyKey: "bar",
				Strict:         true,
				Value: &grpcApi.Value{
					Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
					Data:    []byte("cancel"),
				},
			},
			req: &t_api.Request{
				Kind: t_api.RejectPromise,
				CompletePromise: &t_api.CompletePromiseRequest{
					Id:             "foo",
					IdempotencyKey: util.ToPointer(idempotency.Key("bar")),
					Strict:         true,
					State:          promise.Rejected,
					Value: promise.Value{
						Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
						Data:    []byte("cancel"),
					},
				},
			},
			res: &t_api.Response{
				Kind: t_api.RejectPromise,
				CompletePromise: &t_api.CompletePromiseResponse{
					Status: t_api.StatusCreated,
					Promise: &promise.Promise{
						Id:    "foo",
						State: promise.Rejected,
					},
				},
			},
			noop: false,
		},
		{
			name: "RejectPromiseMinimal",
			grpcReq: &grpcApi.RejectPromiseRequest{
				Id: "foo",
			},
			req: &t_api.Request{
				Kind: t_api.RejectPromise,
				CompletePromise: &t_api.CompletePromiseRequest{
					Id:             "foo",
					IdempotencyKey: nil,
					Strict:         false,
					State:          promise.Rejected,
					Value: promise.Value{
						Headers: nil,
						Data:    nil,
					},
				},
			},
			res: &t_api.Response{
				Kind: t_api.RejectPromise,
				CompletePromise: &t_api.CompletePromiseResponse{
					Status: t_api.StatusCreated,
					Promise: &promise.Promise{
						Id:    "foo",
						State: promise.Rejected,
					},
				},
			},
			noop: false,
		},
		{
			name: "RejectPromiseAlreadyResolved",
			grpcReq: &grpcApi.RejectPromiseRequest{
				Id: "foo",
			},
			req: &t_api.Request{
				Kind: t_api.RejectPromise,
				CompletePromise: &t_api.CompletePromiseRequest{
					Id:             "foo",
					IdempotencyKey: nil,
					Strict:         false,
					State:          promise.Rejected,
					Value: promise.Value{
						Headers: nil,
						Data:    nil,
					},
				},
			},
			res: &t_api.Response{
				Kind: t_api.RejectPromise,
				CompletePromise: &t_api.CompletePromiseResponse{
					Status: t_api.StatusPromiseAlreadyResolved,
					Promise: &promise.Promise{
						Id:    "foo",
						State: promise.Resolved,
					},
				},
			},
			noop:        false,
			expectedErr: codes.PermissionDenied,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			grpcTest.Load(t, tc.req, tc.res)

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			res, err := grpcTest.client.RejectPromise(ctx, tc.grpcReq)
			if err != nil {
				s, ok := status.FromError(err)
				if !ok {
					t.Fatal(err)
				}
				assert.Equal(t, tc.expectedErr, s.Code())
				return
			}

			assert.Equal(t, tc.noop, res.Noop)

			select {
			case err := <-grpcTest.errors:
				t.Fatal(err)
			default:
			}
		})
	}

	if err := grpcTest.teardown(); err != nil {
		t.Fatal(err)
	}
}

// SCHEDULE

func TestCreateSchedule(t *testing.T) {
	grpcTest, err := setup()
	if err != nil {
		t.Fatal(err)
	}

	tcs := []struct {
		name        string
		grpcReq     *grpcApi.CreateScheduleRequest
		req         *t_api.Request
		res         *t_api.Response
		noop        bool
		expectedErr codes.Code
	}{
		{
			name: "CreateSchedule",
			grpcReq: &grpcApi.CreateScheduleRequest{
				Id:             "foo",
				Description:    "bar",
				Cron:           "* * * * *",
				Tags:           map[string]string{"a": "a", "b": "b", "c": "c"},
				PromiseId:      "foo",
				PromiseTimeout: 1,
				PromiseParam: &grpcApi.PromiseValue{
					Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
					Data:    []byte("pending"),
				},
				PromiseTags:    map[string]string{"a": "a", "b": "b", "c": "c"},
				IdempotencyKey: "bar",
			},
			req: &t_api.Request{
				Kind: t_api.CreateSchedule,
				CreateSchedule: &t_api.CreateScheduleRequest{
					Id:             "foo",
					Description:    "bar",
					Cron:           "* * * * *",
					Tags:           map[string]string{"a": "a", "b": "b", "c": "c"},
					PromiseId:      "foo",
					PromiseTimeout: 1,
					PromiseParam: promise.Value{
						Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
						Data:    []byte("pending"),
					},
					PromiseTags:    map[string]string{"a": "a", "b": "b", "c": "c"},
					IdempotencyKey: util.ToPointer(idempotency.Key("bar")),
				},
			},
			res: &t_api.Response{
				Kind: t_api.CreateSchedule,
				CreateSchedule: &t_api.CreateScheduleResponse{
					Status: t_api.StatusCreated,
					Schedule: &schedule.Schedule{
						Id:          "foo",
						Description: "bar",
					},
				},
			},
			noop: false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			grpcTest.Load(t, tc.req, tc.res)

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			resp, err := grpcTest.scheduleClient.CreateSchedule(ctx, tc.grpcReq)
			if err != nil {
				s, ok := status.FromError(err)
				if !ok {
					t.Fatal(err)
				}
				assert.Equal(t, tc.expectedErr, s.Code())
				return
			}

			assert.Equal(t, tc.noop, resp.Noop)

			select {
			case err := <-grpcTest.errors:
				t.Fatal(err)
			default:
			}
		})
	}

	if err := grpcTest.teardown(); err != nil {
		t.Fatal(err)
	}
}

func TestReadSchedule(t *testing.T) {
	grpcTest, err := setup()
	if err != nil {
		t.Fatal(err)
	}

	tcs := []struct {
		name    string
		grpcReq *grpcApi.ReadScheduleRequest
		req     *t_api.Request
		res     *t_api.Response
		code    codes.Code
	}{
		{
			name: "ReadSchedule",
			grpcReq: &grpcApi.ReadScheduleRequest{
				Id: "foo",
			},
			req: &t_api.Request{
				Kind: t_api.ReadSchedule,
				ReadSchedule: &t_api.ReadScheduleRequest{
					Id: "foo",
				},
			},
			res: &t_api.Response{
				Kind: t_api.ReadSchedule,
				ReadSchedule: &t_api.ReadScheduleResponse{
					Status: t_api.StatusOK,
					Schedule: &schedule.Schedule{
						Id:          "foo",
						Description: "bar",
					},
				},
			},
			code: codes.OK,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			grpcTest.Load(t, tc.req, tc.res)

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			_, err := grpcTest.scheduleClient.ReadSchedule(ctx, tc.grpcReq)
			if err != nil {
				s, ok := status.FromError(err)
				if !ok {
					t.Fatal(err)
				}
				assert.Equal(t, tc.code, s.Code())
				return
			}

			assert.Equal(t, tc.code, codes.OK)

			select {
			case err := <-grpcTest.errors:
				t.Fatal(err)
			default:
			}
		})
	}

	if err := grpcTest.teardown(); err != nil {
		t.Fatal(err)
	}
}

func TestSearchSchedule(t *testing.T) {
	grpcTest, err := setup()
	if err != nil {
		t.Fatal(err)
	}

	tcs := []struct {
		name    string
		grpcReq *grpcApi.SearchSchedulesRequest
		req     *t_api.Request
		res     *t_api.Response
	}{
		{
			name: "SearchSchedules",
			grpcReq: &grpcApi.SearchSchedulesRequest{
				Id:    "*",
				Limit: 10,
			},
			req: &t_api.Request{
				Kind: t_api.SearchSchedules,
				SearchSchedules: &t_api.SearchSchedulesRequest{
					Id:    "*",
					Limit: 10,
				},
			},
			res: &t_api.Response{
				Kind: t_api.SearchSchedules,
				SearchSchedules: &t_api.SearchSchedulesResponse{
					Status:    t_api.StatusOK,
					Cursor:    nil,
					Schedules: []*schedule.Schedule{},
				},
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			grpcTest.Load(t, tc.req, tc.res)

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			_, err := grpcTest.scheduleClient.SearchSchedules(ctx, tc.grpcReq)
			if err != nil {
				t.Fatal(err)
			}

			select {
			case err := <-grpcTest.errors:
				t.Fatal(err)
			default:
			}
		})
	}

	if err := grpcTest.teardown(); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteSchedule(t *testing.T) {
	grpcTest, err := setup()
	if err != nil {
		t.Fatal(err)
	}

	tcs := []struct {
		name    string
		grpcReq *grpcApi.DeleteScheduleRequest
		req     *t_api.Request
		res     *t_api.Response
		code    codes.Code
	}{
		{
			name: "DeleteSchedule",
			grpcReq: &grpcApi.DeleteScheduleRequest{
				Id: "foo",
			},
			req: &t_api.Request{
				Kind: t_api.DeleteSchedule,
				DeleteSchedule: &t_api.DeleteScheduleRequest{
					Id: "foo",
				},
			},
			res: &t_api.Response{
				Kind: t_api.DeleteSchedule,
				DeleteSchedule: &t_api.DeleteScheduleResponse{
					Status: t_api.StatusNoContent,
				},
			},
			code: codes.OK,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			grpcTest.Load(t, tc.req, tc.res)

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			_, err := grpcTest.scheduleClient.DeleteSchedule(ctx, tc.grpcReq)
			if err != nil {
				s, ok := status.FromError(err)
				if !ok {
					t.Fatal(err)
				}
				assert.Equal(t, tc.code, s.Code())
				return
			}

			assert.Equal(t, tc.code, codes.OK)

			select {
			case err := <-grpcTest.errors:
				t.Fatal(err)
			default:
			}
		})
	}

	if err := grpcTest.teardown(); err != nil {
		t.Fatal(err)
	}
}

// LOCKS

func TestAcquireLock(t *testing.T) {
	grpcTest, err := setup()
	if err != nil {
		t.Fatal(err)
	}

	tcs := []struct {
		name    string
		grpcReq *grpcApi.AcquireLockRequest
		req     *t_api.Request
		res     *t_api.Response
		code    codes.Code
	}{
		{
			name: "AcquireLock",
			grpcReq: &grpcApi.AcquireLockRequest{
				Lock: &grpcApi.Lock{
					ResourceId:      "foo",
					ProcessId:       "bar",
					ExecutionId:     "baz",
					ExpiryInSeconds: 1,
				},
			},
			req: &t_api.Request{
				Kind: t_api.AcquireLock,
				AcquireLock: &t_api.AcquireLockRequest{
					ResourceId:      "foo",
					ProcessId:       "bar",
					ExecutionId:     "baz",
					ExpiryInSeconds: 1,
				},
			},
			res: &t_api.Response{
				Kind: t_api.AcquireLock,
				AcquireLock: &t_api.AcquireLockResponse{
					Status: t_api.StatusCreated,
					Lock: &lock.Lock{
						ResourceId:      "foo",
						ProcessId:       "bar",
						ExecutionId:     "baz",
						ExpiryInSeconds: 1,
					},
				},
			},
			code: codes.OK,
		},
		{
			name: "AcquireLock missing resource id",
			grpcReq: &grpcApi.AcquireLockRequest{
				Lock: &grpcApi.Lock{
					ResourceId:      "",
					ProcessId:       "bar",
					ExecutionId:     "baz",
					ExpiryInSeconds: 1,
				},
			},
			req:  nil,
			res:  nil,
			code: codes.InvalidArgument,
		},
		{
			name: "AcquireLock missing process id",
			grpcReq: &grpcApi.AcquireLockRequest{
				Lock: &grpcApi.Lock{
					ResourceId:      "foo",
					ProcessId:       "",
					ExecutionId:     "baz",
					ExpiryInSeconds: 1,
				},
			},
			req:  nil,
			res:  nil,
			code: codes.InvalidArgument,
		},
		{
			name: "AcquireLock missing execution id",
			grpcReq: &grpcApi.AcquireLockRequest{
				Lock: &grpcApi.Lock{
					ResourceId:      "foo",
					ProcessId:       "bar",
					ExecutionId:     "",
					ExpiryInSeconds: 1,
				},
			},
			req:  nil,
			res:  nil,
			code: codes.InvalidArgument,
		},
		{
			name: "AcquireLock missing timeout",
			grpcReq: &grpcApi.AcquireLockRequest{
				Lock: &grpcApi.Lock{
					ResourceId:      "foo",
					ProcessId:       "bar",
					ExecutionId:     "baz",
					ExpiryInSeconds: 0,
				},
			},
			req:  nil,
			res:  nil,
			code: codes.InvalidArgument,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			grpcTest.Load(t, tc.req, tc.res)

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			_, err := grpcTest.lockClient.AcquireLock(ctx, tc.grpcReq)
			if err != nil {
				s, ok := status.FromError(err)
				if !ok {
					t.Fatal(err)
				}
				assert.Equal(t, tc.code, s.Code())
				return
			}

			assert.Equal(t, tc.code, codes.OK)

			select {
			case err := <-grpcTest.errors:
				t.Fatal(err)
			default:
			}
		})
	}

	if err := grpcTest.teardown(); err != nil {
		t.Fatal(err)
	}
}

func TestHeartbeatLocks(t *testing.T) {
	grpcTest, err := setup()
	if err != nil {
		t.Fatal(err)
	}

	tcs := []struct {
		name    string
		grpcReq *grpcApi.HeartbeatLocksRequest
		req     *t_api.Request
		res     *t_api.Response
		code    codes.Code
	}{
		{
			name: "HeartbeatLocks",
			grpcReq: &grpcApi.HeartbeatLocksRequest{
				ProcessId: "foo",
			},
			req: &t_api.Request{
				Kind: t_api.HeartbeatLocks,
				HeartbeatLocks: &t_api.HeartbeatLocksRequest{
					ProcessId: "foo",
				},
			},
			res: &t_api.Response{
				Kind: t_api.HeartbeatLocks,
				HeartbeatLocks: &t_api.HeartbeatLocksResponse{
					Status: t_api.StatusOK,
				},
			},
			code: codes.OK,
		},
		{
			name: "HeartbeatLocks missing process id",
			grpcReq: &grpcApi.HeartbeatLocksRequest{
				ProcessId: "",
			},
			req:  nil,
			res:  nil,
			code: codes.InvalidArgument,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			grpcTest.Load(t, tc.req, tc.res)

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			_, err := grpcTest.lockClient.HeartbeatLocks(ctx, tc.grpcReq)
			if err != nil {
				s, ok := status.FromError(err)
				if !ok {
					t.Fatal(err)
				}
				assert.Equal(t, tc.code, s.Code())
				return
			}

			assert.Equal(t, tc.code, codes.OK)

			select {
			case err := <-grpcTest.errors:
				t.Fatal(err)
			default:
			}
		})
	}

	if err := grpcTest.teardown(); err != nil {
		t.Fatal(err)
	}
}

func TestReleaseLock(t *testing.T) {
	grpcTest, err := setup()
	if err != nil {
		t.Fatal(err)
	}

	tcs := []struct {
		name    string
		grpcReq *grpcApi.ReleaseLockRequest
		req     *t_api.Request
		res     *t_api.Response
		code    codes.Code
	}{
		{
			name: "ReleaseLock",
			grpcReq: &grpcApi.ReleaseLockRequest{
				ResourceId:  "foo",
				ExecutionId: "bar",
			},
			req: &t_api.Request{
				Kind: t_api.ReleaseLock,
				ReleaseLock: &t_api.ReleaseLockRequest{
					ResourceId:  "foo",
					ExecutionId: "bar",
				},
			},
			res: &t_api.Response{
				Kind: t_api.ReleaseLock,
				ReleaseLock: &t_api.ReleaseLockResponse{
					Status: t_api.StatusOK,
				},
			},
		},
		{
			name: "ReleaseLock missing resource id",
			grpcReq: &grpcApi.ReleaseLockRequest{
				ResourceId:  "",
				ExecutionId: "bar",
			},
			req:  nil,
			res:  nil,
			code: codes.InvalidArgument,
		},
		{
			name: "ReleaseLock missing execution id",
			grpcReq: &grpcApi.ReleaseLockRequest{
				ResourceId:  "foo",
				ExecutionId: "",
			},
			req:  nil,
			res:  nil,
			code: codes.InvalidArgument,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			grpcTest.Load(t, tc.req, tc.res)

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			_, err := grpcTest.lockClient.ReleaseLock(ctx, tc.grpcReq)
			if err != nil {
				s, ok := status.FromError(err)
				if !ok {
					t.Fatal(err)
				}
				assert.Equal(t, tc.code, s.Code())
				return
			}

			assert.Equal(t, tc.code, codes.OK)

			select {
			case err := <-grpcTest.errors:
				t.Fatal(err)
			default:
			}
		})
	}

	if err := grpcTest.teardown(); err != nil {
		t.Fatal(err)
	}
}

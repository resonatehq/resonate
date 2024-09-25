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
	"github.com/resonatehq/resonate/pkg/message"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/schedule"
	"github.com/resonatehq/resonate/pkg/task"

	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type grpcTest struct {
	*test.API
	subsystem api.Subsystem
	errors    chan error
	conn      *grpc.ClientConn
	promises  grpcApi.PromisesClient
	callbacks grpcApi.CallbacksClient
	schedules grpcApi.SchedulesClient
	locks     grpcApi.LocksClient
	tasks     grpcApi.TasksClient
}

func setup() (*grpcTest, error) {
	api := &test.API{}
	errors := make(chan error)
	subsystem := New(api, &Config{
		Host: "127.0.0.1",
		Port: 5555,
	})

	// start grpc server
	go subsystem.Start(errors)
	time.Sleep(100 * time.Millisecond)

	conn, err := grpc.NewClient("127.0.0.1:5555", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return &grpcTest{
		API:       api,
		subsystem: subsystem,
		errors:    errors,
		conn:      conn,
		promises:  grpcApi.NewPromisesClient(conn),
		callbacks: grpcApi.NewCallbacksClient(conn),
		schedules: grpcApi.NewSchedulesClient(conn),
		locks:     grpcApi.NewLocksClient(conn),
		tasks:     grpcApi.NewTasksClient(conn),
	}, nil
}

func (t *grpcTest) teardown() error {
	defer close(t.errors)

	if err := t.conn.Close(); err != nil {
		return err
	}

	return t.subsystem.Stop()
}

// Promises

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
				RequestId: "ReadPromise",
				Id:        "foo",
			},
			req: &t_api.Request{
				Kind: t_api.ReadPromise,
				Tags: map[string]string{
					"id":       "ReadPromise",
					"name":     "ReadPromise",
					"protocol": "grpc",
				},
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
				RequestId: "ReadPromiseNotFound",
				Id:        "bar",
			},
			req: &t_api.Request{
				Kind: t_api.ReadPromise,
				Tags: map[string]string{
					"id":       "ReadPromiseNotFound",
					"name":     "ReadPromise",
					"protocol": "grpc",
				},
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

			_, err := grpcTest.promises.ReadPromise(ctx, tc.grpcReq)
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
				RequestId: "SearchPromises",
				Id:        "*",
				Limit:     10,
			},
			req: &t_api.Request{
				Kind: t_api.SearchPromises,
				Tags: map[string]string{
					"id":       "SearchPromises",
					"name":     "SearchPromises",
					"protocol": "grpc",
				},
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
				RequestId: "SearchPromisesCursor",
				Cursor:    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJOZXh0Ijp7ImlkIjoiKiIsInN0YXRlcyI6WyJQRU5ESU5HIl0sInRhZ3MiOnt9LCJsaW1pdCI6MTAsInNvcnRJZCI6MTAwfX0.XKusWO-Jl4v7QVIwh5Pn3oIElBvtpf0VPOLJkXPvQLk",
			},
			req: &t_api.Request{
				Kind: t_api.SearchPromises,
				Tags: map[string]string{
					"id":       "SearchPromisesCursor",
					"name":     "SearchPromises",
					"protocol": "grpc",
				},
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
				RequestId: "SearchPromisesPending",
				Id:        "*",
				State:     grpcApi.SearchState_SEARCH_PENDING,
				Limit:     10,
			},
			req: &t_api.Request{
				Kind: t_api.SearchPromises,
				Tags: map[string]string{
					"id":       "SearchPromisesPending",
					"name":     "SearchPromises",
					"protocol": "grpc",
				},
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
				RequestId: "SearchPromisesResolved",
				Id:        "*",
				State:     grpcApi.SearchState_SEARCH_RESOLVED,
				Limit:     10,
			},
			req: &t_api.Request{
				Kind: t_api.SearchPromises,
				Tags: map[string]string{
					"id":       "SearchPromisesResolved",
					"name":     "SearchPromises",
					"protocol": "grpc",
				},
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
				RequestId: "SearchPromisesRejected",
				Id:        "*",
				State:     grpcApi.SearchState_SEARCH_REJECTED,
				Limit:     10,
			},
			req: &t_api.Request{
				Kind: t_api.SearchPromises,
				Tags: map[string]string{
					"id":       "SearchPromisesRejected",
					"name":     "SearchPromises",
					"protocol": "grpc",
				},
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
				RequestId: "SearchPromisesTags",
				Id:        "*",
				Tags: map[string]string{
					"resonate:invocation": "true",
				},
				Limit: 10,
			},
			req: &t_api.Request{
				Kind: t_api.SearchPromises,
				Tags: map[string]string{
					"id":       "SearchPromisesTags",
					"name":     "SearchPromises",
					"protocol": "grpc",
				},
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

			_, err := grpcTest.promises.SearchPromises(ctx, tc.grpcReq)
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
				RequestId:      "CreatePromise",
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
				Tags: map[string]string{
					"id":       "CreatePromise",
					"name":     "CreatePromise",
					"protocol": "grpc",
				},
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
				RequestId: "CreatePromiseMinimal",
				Id:        "foo",
				Timeout:   1,
			},
			req: &t_api.Request{
				Kind: t_api.CreatePromise,
				Tags: map[string]string{
					"id":       "CreatePromiseMinimal",
					"name":     "CreatePromise",
					"protocol": "grpc",
				},
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
			name: "CreatePromiseNoTimeout",
			grpcReq: &grpcApi.CreatePromiseRequest{
				RequestId: "CreatePromiseNoTimeout",
				Id:        "foo",
			},
			req: &t_api.Request{
				Kind: t_api.CreatePromise,
				Tags: map[string]string{
					"id":       "CreatePromiseNoTimeout",
					"name":     "CreatePromise",
					"protocol": "grpc",
				},
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

			res, err := grpcTest.promises.CreatePromise(ctx, tc.grpcReq)
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
				RequestId:      "CancelPromise",
				Id:             "foo",
				IdempotencyKey: "bar",
				Strict:         true,
				Value: &grpcApi.Value{
					Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
					Data:    []byte("cancel"),
				},
			},
			req: &t_api.Request{
				Kind: t_api.CompletePromise,
				Tags: map[string]string{
					"id":       "CancelPromise",
					"name":     "CompletePromise",
					"protocol": "grpc",
				},
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
				Kind: t_api.CompletePromise,
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
				RequestId: "CancelPromiseMinimal",
				Id:        "foo",
			},
			req: &t_api.Request{
				Kind: t_api.CompletePromise,
				Tags: map[string]string{
					"id":       "CancelPromiseMinimal",
					"name":     "CompletePromise",
					"protocol": "grpc",
				},
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
				Kind: t_api.CompletePromise,
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
				RequestId: "CancelPromiseAlreadyResolved",
				Id:        "foo",
			},
			req: &t_api.Request{
				Kind: t_api.CompletePromise,
				Tags: map[string]string{
					"id":       "CancelPromiseAlreadyResolved",
					"name":     "CompletePromise",
					"protocol": "grpc",
				},
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
				Kind: t_api.CompletePromise,
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

			res, err := grpcTest.promises.CancelPromise(ctx, tc.grpcReq)
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
				RequestId:      "ResolvePromise",
				Id:             "foo",
				IdempotencyKey: "bar",
				Strict:         true,
				Value: &grpcApi.Value{
					Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
					Data:    []byte("cancel"),
				},
			},
			req: &t_api.Request{
				Kind: t_api.CompletePromise,
				Tags: map[string]string{
					"id":       "ResolvePromise",
					"name":     "CompletePromise",
					"protocol": "grpc",
				},
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
				Kind: t_api.CompletePromise,
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
				RequestId: "ResolvePromiseMinimal",
				Id:        "foo",
			},
			req: &t_api.Request{
				Kind: t_api.CompletePromise,
				Tags: map[string]string{
					"id":       "ResolvePromiseMinimal",
					"name":     "CompletePromise",
					"protocol": "grpc",
				},
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
				Kind: t_api.CompletePromise,
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
				RequestId: "ResolvePromiseAlreadyRejected",
				Id:        "foo",
			},
			req: &t_api.Request{
				Kind: t_api.CompletePromise,
				Tags: map[string]string{
					"id":       "ResolvePromiseAlreadyRejected",
					"name":     "CompletePromise",
					"protocol": "grpc",
				},
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
				Kind: t_api.CompletePromise,
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

			res, err := grpcTest.promises.ResolvePromise(ctx, tc.grpcReq)
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
				RequestId:      "RejectPromise",
				Id:             "foo",
				IdempotencyKey: "bar",
				Strict:         true,
				Value: &grpcApi.Value{
					Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
					Data:    []byte("cancel"),
				},
			},
			req: &t_api.Request{
				Kind: t_api.CompletePromise,
				Tags: map[string]string{
					"id":       "RejectPromise",
					"name":     "CompletePromise",
					"protocol": "grpc",
				},
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
				Kind: t_api.CompletePromise,
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
				RequestId: "RejectPromiseMinimal",
				Id:        "foo",
			},
			req: &t_api.Request{
				Kind: t_api.CompletePromise,
				Tags: map[string]string{
					"id":       "RejectPromiseMinimal",
					"name":     "CompletePromise",
					"protocol": "grpc",
				},
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
				Kind: t_api.CompletePromise,
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
				RequestId: "RejectPromiseAlreadyResolved",
				Id:        "foo",
			},
			req: &t_api.Request{
				Kind: t_api.CompletePromise,
				Tags: map[string]string{
					"id":       "RejectPromiseAlreadyResolved",
					"name":     "CompletePromise",
					"protocol": "grpc",
				},
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
				Kind: t_api.CompletePromise,
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

			res, err := grpcTest.promises.RejectPromise(ctx, tc.grpcReq)
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

// Callbacks

func TestCreateCallback(t *testing.T) {
	grpcTest, err := setup()
	if err != nil {
		t.Fatal(err)
	}

	tcs := []struct {
		name    string
		grpcReq *grpcApi.CreateCallbackRequest
		req     *t_api.Request
		res     *t_api.Response
		code    codes.Code
	}{
		{
			name: "CreateCallback",
			grpcReq: &grpcApi.CreateCallbackRequest{
				PromiseId:     "foo",
				RootPromiseId: "bar",
				Timeout:       1,
				Recv:          &grpcApi.CreateCallbackRequest_Logical{Logical: "foo"},
				RequestId:     "CreateCallback",
			},
			req: &t_api.Request{
				Kind: t_api.CreateCallback,
				Tags: map[string]string{
					"id":       "CreateCallback",
					"name":     "CreateCallback",
					"protocol": "grpc",
				},
				CreateCallback: &t_api.CreateCallbackRequest{
					PromiseId:     "foo",
					RootPromiseId: "bar",
					Timeout:       1,
					Recv:          []byte(`"foo"`),
				},
			},
			res: &t_api.Response{
				Kind: t_api.CreateCallback,
				CreateCallback: &t_api.CreateCallbackResponse{
					Status: t_api.StatusCreated,
				},
			},
			code: codes.OK,
		},
		{
			name: "CreateCallbackPhysicalReceiver",
			grpcReq: &grpcApi.CreateCallbackRequest{
				PromiseId:     "foo",
				RootPromiseId: "bar",
				Timeout:       1,
				Recv:          &grpcApi.CreateCallbackRequest_Physical{Physical: &grpcApi.Recv{Type: "http", Data: []byte(`{"url": "http://localhost:3000"}`)}},
				RequestId:     "CreateCallbackPhysicalReceiver",
			},
			req: &t_api.Request{
				Kind: t_api.CreateCallback,
				Tags: map[string]string{
					"id":       "CreateCallbackPhysicalReceiver",
					"name":     "CreateCallback",
					"protocol": "grpc",
				},
				CreateCallback: &t_api.CreateCallbackRequest{
					PromiseId:     "foo",
					RootPromiseId: "bar",
					Timeout:       1,
					Recv:          []byte(`{"type":"http","data":{"url":"http://localhost:3000"}}`),
				},
			},
			res: &t_api.Response{
				Kind: t_api.CreateCallback,
				CreateCallback: &t_api.CreateCallbackResponse{
					Status: t_api.StatusCreated,
				},
			},
			code: codes.OK,
		},
		{
			name: "CreateCallbackNoRecv",
			grpcReq: &grpcApi.CreateCallbackRequest{
				PromiseId:     "foo",
				RootPromiseId: "bar",
				Timeout:       1,
				RequestId:     "CreateCallbackNoRecv",
			},
			req:  nil,
			res:  nil,
			code: codes.InvalidArgument,
		},
		{
			name: "CreateCallbackInvalidPhysicalReceiver",
			grpcReq: &grpcApi.CreateCallbackRequest{
				PromiseId:     "foo",
				RootPromiseId: "bar",
				Timeout:       1,
				Recv:          &grpcApi.CreateCallbackRequest_Physical{Physical: &grpcApi.Recv{Type: "http", Data: []byte("nope")}},
				RequestId:     "CreateCallbackInvalidPhysicalReceiver",
			},
			req:  nil,
			res:  nil,
			code: codes.InvalidArgument,
		},
		{
			name: "CreateCallbackNotFound",
			grpcReq: &grpcApi.CreateCallbackRequest{
				PromiseId: "foo",
				Timeout:   1,
				Recv:      &grpcApi.CreateCallbackRequest_Logical{Logical: "foo"},
				RequestId: "CreateCallbackNotFound",
			},
			req: &t_api.Request{
				Kind: t_api.CreateCallback,
				Tags: map[string]string{
					"id":       "CreateCallbackNotFound",
					"name":     "CreateCallback",
					"protocol": "grpc",
				},
				CreateCallback: &t_api.CreateCallbackRequest{
					PromiseId: "foo",
					Timeout:   1,
					Recv:      []byte(`"foo"`),
				},
			},
			res: &t_api.Response{
				Kind: t_api.CreateCallback,
				CreateCallback: &t_api.CreateCallbackResponse{
					Status: t_api.StatusPromiseNotFound,
				},
			},
			code: codes.NotFound,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			grpcTest.Load(t, tc.req, tc.res)

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			_, err := grpcTest.callbacks.CreateCallback(ctx, tc.grpcReq)
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

// Schedules

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
				RequestId:      "CreateSchedule",
				Id:             "foo",
				Description:    "bar",
				Cron:           "* * * * *",
				Tags:           map[string]string{"a": "a", "b": "b", "c": "c"},
				PromiseId:      "foo",
				PromiseTimeout: 1,
				PromiseParam: &grpcApi.Value{
					Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
					Data:    []byte("pending"),
				},
				PromiseTags:    map[string]string{"a": "a", "b": "b", "c": "c"},
				IdempotencyKey: "bar",
			},
			req: &t_api.Request{
				Kind: t_api.CreateSchedule,
				Tags: map[string]string{
					"id":       "CreateSchedule",
					"name":     "CreateSchedule",
					"protocol": "grpc",
				},
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

			res, err := grpcTest.schedules.CreateSchedule(ctx, tc.grpcReq)
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
				RequestId: "ReadSchedule",
				Id:        "foo",
			},
			req: &t_api.Request{
				Kind: t_api.ReadSchedule,
				Tags: map[string]string{
					"id":       "ReadSchedule",
					"name":     "ReadSchedule",
					"protocol": "grpc",
				},
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

			_, err := grpcTest.schedules.ReadSchedule(ctx, tc.grpcReq)
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
				RequestId: "SearchSchedules",
				Id:        "*",
				Limit:     10,
			},
			req: &t_api.Request{
				Kind: t_api.SearchSchedules,
				Tags: map[string]string{
					"id":       "SearchSchedules",
					"name":     "SearchSchedules",
					"protocol": "grpc",
				},
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

			_, err := grpcTest.schedules.SearchSchedules(ctx, tc.grpcReq)
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
				RequestId: "DeleteSchedule",
				Id:        "foo",
			},
			req: &t_api.Request{
				Kind: t_api.DeleteSchedule,
				Tags: map[string]string{
					"id":       "DeleteSchedule",
					"name":     "DeleteSchedule",
					"protocol": "grpc",
				},
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

			_, err := grpcTest.schedules.DeleteSchedule(ctx, tc.grpcReq)
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

// Locks

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
				RequestId:            "AcquireLock",
				ResourceId:           "foo",
				ProcessId:            "bar",
				ExecutionId:          "baz",
				ExpiryInMilliseconds: 1,
			},
			req: &t_api.Request{
				Kind: t_api.AcquireLock,
				Tags: map[string]string{
					"id":       "AcquireLock",
					"name":     "AcquireLock",
					"protocol": "grpc",
				},
				AcquireLock: &t_api.AcquireLockRequest{
					ResourceId:           "foo",
					ProcessId:            "bar",
					ExecutionId:          "baz",
					ExpiryInMilliseconds: 1,
				},
			},
			res: &t_api.Response{
				Kind: t_api.AcquireLock,
				AcquireLock: &t_api.AcquireLockResponse{
					Status: t_api.StatusCreated,
					Lock: &lock.Lock{
						ResourceId:           "foo",
						ProcessId:            "bar",
						ExecutionId:          "baz",
						ExpiryInMilliseconds: 1,
					},
				},
			},
			code: codes.OK,
		},
		{
			name: "AcquireLockNoResourceId",
			grpcReq: &grpcApi.AcquireLockRequest{
				ResourceId:           "",
				ProcessId:            "bar",
				ExecutionId:          "baz",
				ExpiryInMilliseconds: 1,
			},
			req:  nil,
			res:  nil,
			code: codes.InvalidArgument,
		},
		{
			name: "AcquireLockNoProcessId",
			grpcReq: &grpcApi.AcquireLockRequest{
				ResourceId:           "foo",
				ProcessId:            "",
				ExecutionId:          "baz",
				ExpiryInMilliseconds: 1,
			},
			req:  nil,
			res:  nil,
			code: codes.InvalidArgument,
		},
		{
			name: "AcquireLockNoExecutionId",
			grpcReq: &grpcApi.AcquireLockRequest{
				ProcessId:            "bar",
				ExecutionId:          "",
				ExpiryInMilliseconds: 1,
			},
			req:  nil,
			res:  nil,
			code: codes.InvalidArgument,
		},
		{
			name: "AcquireLockNoTimeout",
			grpcReq: &grpcApi.AcquireLockRequest{
				ResourceId:           "foo",
				ProcessId:            "bar",
				ExecutionId:          "baz",
				ExpiryInMilliseconds: 0,
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

			_, err := grpcTest.locks.AcquireLock(ctx, tc.grpcReq)
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
				RequestId:   "ReleaseLock",
				ResourceId:  "foo",
				ExecutionId: "bar",
			},
			req: &t_api.Request{
				Kind: t_api.ReleaseLock,
				Tags: map[string]string{
					"id":       "ReleaseLock",
					"name":     "ReleaseLock",
					"protocol": "grpc",
				},
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
			name: "ReleaseLockNoResourceId",
			grpcReq: &grpcApi.ReleaseLockRequest{
				ResourceId:  "",
				ExecutionId: "bar",
			},
			req:  nil,
			res:  nil,
			code: codes.InvalidArgument,
		},
		{
			name: "ReleaseLockNoExecutionId",
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

			_, err := grpcTest.locks.ReleaseLock(ctx, tc.grpcReq)
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
				RequestId: "HeartbeatLocks",
				ProcessId: "foo",
			},
			req: &t_api.Request{
				Kind: t_api.HeartbeatLocks,
				Tags: map[string]string{
					"id":       "HeartbeatLocks",
					"name":     "HeartbeatLocks",
					"protocol": "grpc",
				},
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
			name: "HeartbeatLocksNoProcessId",
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

			_, err := grpcTest.locks.HeartbeatLocks(ctx, tc.grpcReq)
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

// Tasks

func TestClaimTask(t *testing.T) {
	grpcTest, err := setup()
	if err != nil {
		t.Fatal(err)
	}

	tcs := []struct {
		name    string
		grpcReq *grpcApi.ClaimTaskRequest
		grpcRes *grpcApi.ClaimTaskResponse
		req     *t_api.Request
		res     *t_api.Response
		code    codes.Code
	}{
		{
			name: "ClaimTask",
			grpcReq: &grpcApi.ClaimTaskRequest{
				Id:        "foo",
				ProcessId: "bar",
				Counter:   0,
				Frequency: 1,
				RequestId: "ClaimTask",
			},
			req: &t_api.Request{
				Kind: t_api.ClaimTask,
				Tags: map[string]string{
					"id":       "ClaimTask",
					"name":     "ClaimTask",
					"protocol": "grpc",
				},
				ClaimTask: &t_api.ClaimTaskRequest{
					Id:        "foo",
					ProcessId: "bar",
					Counter:   0,
					Frequency: 1,
				},
			},
			res: &t_api.Response{
				Kind: t_api.ClaimTask,
				ClaimTask: &t_api.ClaimTaskResponse{
					Status: t_api.StatusCreated,
					Task:   &task.Task{Mesg: &message.Mesg{}},
				},
			},
			code: codes.OK,
		},
		{
			name: "ClaimTaskInvoke",
			grpcReq: &grpcApi.ClaimTaskRequest{
				Id:        "foo",
				ProcessId: "bar",
				Counter:   1,
				Frequency: 1,
				RequestId: "ClaimTaskInvoke",
			},
			grpcRes: &grpcApi.ClaimTaskResponse{
				Claimed: true,
				Mesg: &grpcApi.Mesg{
					Type: "invoke",
					Promises: map[string]*grpcApi.Promise{
						"root": {Id: "foo", State: grpcApi.State_PENDING, Param: &grpcApi.Value{}, Value: &grpcApi.Value{}},
					},
				},
			},
			req: &t_api.Request{
				Kind: t_api.ClaimTask,
				Tags: map[string]string{
					"id":       "ClaimTaskInvoke",
					"name":     "ClaimTask",
					"protocol": "grpc",
				},
				ClaimTask: &t_api.ClaimTaskRequest{
					Id:        "foo",
					ProcessId: "bar",
					Counter:   1,
					Frequency: 1,
				},
			},
			res: &t_api.Response{
				Kind: t_api.ClaimTask,
				ClaimTask: &t_api.ClaimTaskResponse{
					Status: t_api.StatusCreated,
					Task: &task.Task{
						Mesg: &message.Mesg{
							Type:     message.Invoke,
							Promises: map[string]*promise.Promise{"root": {Id: "foo", State: promise.Pending}},
						},
					},
				},
			},
			code: codes.OK,
		},
		{
			name: "ClaimTaskResume",
			grpcReq: &grpcApi.ClaimTaskRequest{
				Id:        "foo",
				ProcessId: "bar",
				Counter:   2,
				Frequency: 1,
				RequestId: "ClaimTaskResume",
			},
			grpcRes: &grpcApi.ClaimTaskResponse{
				Claimed: true,
				Mesg: &grpcApi.Mesg{
					Type: "resume",
					Promises: map[string]*grpcApi.Promise{
						"root": {Id: "foo", State: grpcApi.State_PENDING, Param: &grpcApi.Value{}, Value: &grpcApi.Value{}},
						"leaf": {Id: "bar", State: grpcApi.State_RESOLVED, Param: &grpcApi.Value{}, Value: &grpcApi.Value{}},
					},
				},
			},
			req: &t_api.Request{
				Kind: t_api.ClaimTask,
				Tags: map[string]string{
					"id":       "ClaimTaskResume",
					"name":     "ClaimTask",
					"protocol": "grpc",
				},
				ClaimTask: &t_api.ClaimTaskRequest{
					Id:        "foo",
					ProcessId: "bar",
					Counter:   2,
					Frequency: 1,
				},
			},
			res: &t_api.Response{
				Kind: t_api.ClaimTask,
				ClaimTask: &t_api.ClaimTaskResponse{
					Status: t_api.StatusCreated,
					Task: &task.Task{
						Mesg: &message.Mesg{
							Type:     message.Resume,
							Promises: map[string]*promise.Promise{"root": {Id: "foo", State: promise.Pending}, "leaf": {Id: "bar", State: promise.Resolved}},
						},
					},
				},
			},
			code: codes.OK,
		},
		{
			name: "ClaimTaskNoId",
			grpcReq: &grpcApi.ClaimTaskRequest{
				ProcessId: "bar",
				Counter:   0,
				Frequency: 1,
			},
			req:  nil,
			res:  nil,
			code: codes.InvalidArgument,
		},
		{
			name: "ClaimTaskNoProcessId",
			grpcReq: &grpcApi.ClaimTaskRequest{
				Id:        "foo",
				Counter:   0,
				Frequency: 1,
			},
			req:  nil,
			res:  nil,
			code: codes.InvalidArgument,
		},
		{
			name: "ClaimTaskNoFrequency",
			grpcReq: &grpcApi.ClaimTaskRequest{
				Id:        "foo",
				ProcessId: "bar",
				Counter:   0,
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

			res, err := grpcTest.tasks.ClaimTask(ctx, tc.grpcReq)
			if err != nil {
				s, ok := status.FromError(err)
				if !ok {
					t.Fatal(err)
				}
				assert.Equal(t, tc.code, s.Code())
				return
			}

			assert.Equal(t, tc.code, codes.OK)

			if tc.grpcRes != nil {
				assert.Equal(t, tc.grpcRes.Claimed, res.Claimed)
				assert.Equal(t, tc.grpcRes.Mesg, res.Mesg)
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

func TestCompleteTask(t *testing.T) {
	grpcTest, err := setup()
	if err != nil {
		t.Fatal(err)
	}

	tcs := []struct {
		name    string
		grpcReq *grpcApi.CompleteTaskRequest
		req     *t_api.Request
		res     *t_api.Response
		code    codes.Code
	}{
		{
			name: "CompleteTask",
			grpcReq: &grpcApi.CompleteTaskRequest{
				Id:        "foo",
				Counter:   0,
				RequestId: "CompleteTask",
			},
			req: &t_api.Request{
				Kind: t_api.CompleteTask,
				Tags: map[string]string{
					"id":       "CompleteTask",
					"name":     "CompleteTask",
					"protocol": "grpc",
				},
				CompleteTask: &t_api.CompleteTaskRequest{
					Id:      "foo",
					Counter: 0,
				},
			},
			res: &t_api.Response{
				Kind: t_api.CompleteTask,
				CompleteTask: &t_api.CompleteTaskResponse{
					Status: t_api.StatusCreated,
				},
			},
			code: codes.OK,
		},
		{
			name: "CompleteTaskNoId",
			grpcReq: &grpcApi.CompleteTaskRequest{
				Counter: 0,
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

			_, err := grpcTest.tasks.CompleteTask(ctx, tc.grpcReq)
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

func TestHeartbeatTasks(t *testing.T) {
	grpcTest, err := setup()
	if err != nil {
		t.Fatal(err)
	}

	tcs := []struct {
		name    string
		grpcReq *grpcApi.HeartbeatTasksRequest
		req     *t_api.Request
		res     *t_api.Response
		code    codes.Code
	}{
		{
			name: "HeartbeatTasks",
			grpcReq: &grpcApi.HeartbeatTasksRequest{
				ProcessId: "bar",
				RequestId: "HeartbeatTasks",
			},
			req: &t_api.Request{
				Kind: t_api.HeartbeatTasks,
				Tags: map[string]string{
					"id":       "HeartbeatTasks",
					"name":     "HeartbeatTasks",
					"protocol": "grpc",
				},
				HeartbeatTasks: &t_api.HeartbeatTasksRequest{
					ProcessId: "bar",
				},
			},
			res: &t_api.Response{
				Kind: t_api.HeartbeatTasks,
				HeartbeatTasks: &t_api.HeartbeatTasksResponse{
					Status:        t_api.StatusOK,
					TasksAffected: 1,
				},
			},
			code: codes.OK,
		},
		{
			name:    "HeartbeatTasksNoProcessId",
			grpcReq: &grpcApi.HeartbeatTasksRequest{},
			req:     nil,
			res:     nil,
			code:    codes.InvalidArgument,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			grpcTest.Load(t, tc.req, tc.res)

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			_, err := grpcTest.tasks.HeartbeatTasks(ctx, tc.grpcReq)
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

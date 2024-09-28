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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
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
	subsystem, err := New(api, &Config{Addr: ":0"})
	if err != nil {
		return nil, err
	}

	// start grpc server
	go subsystem.Start(errors)
	time.Sleep(100 * time.Millisecond)

	conn, err := grpc.NewClient(subsystem.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
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

func TestGrpc(t *testing.T) {
	grpcTest, err := setup()
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name    string
		grpcReq protoreflect.ProtoMessage
		grpcRes protoreflect.ProtoMessage
		req     *t_api.Request
		res     *t_api.Response
		code    codes.Code // grpc error code
	}{
		// Promises
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
			grpcRes: &grpcApi.CreatePromiseResponse{
				Noop: false,
				Promise: &grpcApi.Promise{
					Id:                      "foo",
					State:                   grpcApi.State_PENDING,
					IdempotencyKeyForCreate: "bar",
					Param: &grpcApi.Value{
						Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
						Data:    []byte("pending"),
					},
					Value:   &grpcApi.Value{},
					Timeout: 1,
				},
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
						Id:                      "foo",
						State:                   promise.Pending,
						IdempotencyKeyForCreate: util.ToPointer(idempotency.Key("bar")),
						Param: promise.Value{
							Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
							Data:    []byte("pending"),
						},
						Timeout: 1,
					},
				},
			},
		},
		{
			name: "CreatePromiseMinimal",
			grpcReq: &grpcApi.CreatePromiseRequest{
				RequestId: "CreatePromiseMinimal",
				Id:        "foo",
				Timeout:   1,
			},
			grpcRes: &grpcApi.CreatePromiseResponse{
				Noop: false,
				Promise: &grpcApi.Promise{
					Id:                      "foo",
					State:                   grpcApi.State_PENDING,
					IdempotencyKeyForCreate: "",
					Param:                   &grpcApi.Value{},
					Value:                   &grpcApi.Value{},
					Timeout:                 1,
				},
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
					Param:          promise.Value{},
					Timeout:        1,
				},
			},
			res: &t_api.Response{
				Kind: t_api.CreatePromise,
				CreatePromise: &t_api.CreatePromiseResponse{
					Status: t_api.StatusCreated,
					Promise: &promise.Promise{
						Id:                      "foo",
						State:                   promise.Pending,
						IdempotencyKeyForCreate: nil,
						Param:                   promise.Value{},
						Timeout:                 1,
					},
				},
			},
		},
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
			grpcRes: &grpcApi.ResolvePromiseResponse{
				Noop: false,
				Promise: &grpcApi.Promise{
					Id:                        "foo",
					State:                     grpcApi.State_RESOLVED,
					IdempotencyKeyForComplete: "bar",
					Param:                     &grpcApi.Value{},
					Value: &grpcApi.Value{
						Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
						Data:    []byte("cancel"),
					},
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
						Id:                        "foo",
						State:                     promise.Resolved,
						IdempotencyKeyForComplete: util.ToPointer(idempotency.Key("bar")),
						Value: promise.Value{
							Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
							Data:    []byte("cancel"),
						},
					},
				},
			},
		},
		{
			name: "ResolvePromiseMinimal",
			grpcReq: &grpcApi.ResolvePromiseRequest{
				RequestId: "ResolvePromiseMinimal",
				Id:        "foo",
			},
			grpcRes: &grpcApi.ResolvePromiseResponse{
				Noop: false,
				Promise: &grpcApi.Promise{
					Id:                        "foo",
					State:                     grpcApi.State_RESOLVED,
					IdempotencyKeyForComplete: "",
					Param:                     &grpcApi.Value{},
					Value:                     &grpcApi.Value{},
				},
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
					Value:          promise.Value{},
				},
			},
			res: &t_api.Response{
				Kind: t_api.CompletePromise,
				CompletePromise: &t_api.CompletePromiseResponse{
					Status: t_api.StatusCreated,
					Promise: &promise.Promise{
						Id:                        "foo",
						State:                     promise.Resolved,
						IdempotencyKeyForComplete: nil,
						Value:                     promise.Value{},
					},
				},
			},
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
					Value:          promise.Value{},
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
			code: codes.PermissionDenied,
		},
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
			grpcRes: &grpcApi.RejectPromiseResponse{
				Noop: false,
				Promise: &grpcApi.Promise{
					Id:                        "foo",
					State:                     grpcApi.State_REJECTED,
					IdempotencyKeyForComplete: "bar",
					Param:                     &grpcApi.Value{},
					Value: &grpcApi.Value{
						Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
						Data:    []byte("cancel"),
					},
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
						Id:                        "foo",
						State:                     promise.Rejected,
						IdempotencyKeyForComplete: util.ToPointer(idempotency.Key("bar")),
						Value: promise.Value{
							Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
							Data:    []byte("cancel"),
						},
					},
				},
			},
		},
		{
			name: "RejectPromiseMinimal",
			grpcReq: &grpcApi.RejectPromiseRequest{
				RequestId: "RejectPromiseMinimal",
				Id:        "foo",
			},
			grpcRes: &grpcApi.RejectPromiseResponse{
				Noop: false,
				Promise: &grpcApi.Promise{
					Id:                        "foo",
					State:                     grpcApi.State_REJECTED,
					IdempotencyKeyForComplete: "",
					Param:                     &grpcApi.Value{},
					Value:                     &grpcApi.Value{},
				},
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
					Value:          promise.Value{},
				},
			},
			res: &t_api.Response{
				Kind: t_api.CompletePromise,
				CompletePromise: &t_api.CompletePromiseResponse{
					Status: t_api.StatusCreated,
					Promise: &promise.Promise{
						Id:                        "foo",
						State:                     promise.Rejected,
						IdempotencyKeyForComplete: nil,
						Value:                     promise.Value{},
					},
				},
			},
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
					Value:          promise.Value{},
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
			code: codes.PermissionDenied,
		},
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
			grpcRes: &grpcApi.CancelPromiseResponse{
				Noop: false,
				Promise: &grpcApi.Promise{
					Id:                        "foo",
					State:                     grpcApi.State_REJECTED_CANCELED,
					IdempotencyKeyForComplete: "bar",
					Param:                     &grpcApi.Value{},
					Value: &grpcApi.Value{
						Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
						Data:    []byte("cancel"),
					},
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
						Id:                        "foo",
						State:                     promise.Canceled,
						IdempotencyKeyForComplete: util.ToPointer(idempotency.Key("bar")),
						Value: promise.Value{
							Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
							Data:    []byte("cancel"),
						},
					},
				},
			},
		},
		{
			name: "CancelPromiseMinimal",
			grpcReq: &grpcApi.CancelPromiseRequest{
				RequestId: "CancelPromiseMinimal",
				Id:        "foo",
			},
			grpcRes: &grpcApi.CancelPromiseResponse{
				Noop: false,
				Promise: &grpcApi.Promise{
					Id:                        "foo",
					State:                     grpcApi.State_REJECTED_CANCELED,
					IdempotencyKeyForComplete: "",
					Param:                     &grpcApi.Value{},
					Value:                     &grpcApi.Value{},
				},
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
					Value:          promise.Value{},
				},
			},
			res: &t_api.Response{
				Kind: t_api.CompletePromise,
				CompletePromise: &t_api.CompletePromiseResponse{
					Status: t_api.StatusCreated,
					Promise: &promise.Promise{
						Id:                        "foo",
						State:                     promise.Canceled,
						IdempotencyKeyForComplete: nil,
						Value:                     promise.Value{},
					},
				},
			},
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
					Value:          promise.Value{},
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
			code: codes.PermissionDenied,
		},

		// Callbacks
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

		// Schedules
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
			grpcRes: &grpcApi.CreatedScheduleResponse{
				Noop: false,
				Schedule: &grpcApi.Schedule{
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
			},
		},
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
		},

		// Locks

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
		},
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
		},

		// Tasks
		{
			name: "ClaimTask",
			grpcReq: &grpcApi.ClaimTaskRequest{
				Id:        "foo",
				Counter:   1,
				ProcessId: "bar",
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
					Counter:   1,
					ProcessId: "bar",
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
		},
		{
			name: "ClaimTaskInvoke",
			grpcReq: &grpcApi.ClaimTaskRequest{
				Id:        "foo",
				Counter:   1,
				ProcessId: "bar",
				Frequency: 1,
				RequestId: "ClaimTaskInvoke",
			},
			grpcRes: &grpcApi.ClaimTaskResponse{
				Claimed: true,
				Mesg: &grpcApi.Mesg{
					Type: "invoke",
					Promises: map[string]*grpcApi.PromiseOrHref{
						"root": {Data: &grpcApi.PromiseOrHref_Promise{Promise: &grpcApi.Promise{Id: "foo", State: grpcApi.State_PENDING, Param: &grpcApi.Value{}, Value: &grpcApi.Value{}}}},
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
					Status:          t_api.StatusCreated,
					Task:            &task.Task{Mesg: &message.Mesg{Type: message.Invoke, Root: "foo"}},
					RootPromise:     &promise.Promise{Id: "foo", State: promise.Pending},
					RootPromiseHref: "http://localhost:8001/promises/foo",
				},
			},
		},
		{
			name: "ClaimTaskResume",
			grpcReq: &grpcApi.ClaimTaskRequest{
				Id:        "foo",
				Counter:   2,
				ProcessId: "bar",
				Frequency: 1,
				RequestId: "ClaimTaskResume",
			},
			grpcRes: &grpcApi.ClaimTaskResponse{
				Claimed: true,
				Mesg: &grpcApi.Mesg{
					Type: "resume",
					Promises: map[string]*grpcApi.PromiseOrHref{
						"root": {Data: &grpcApi.PromiseOrHref_Promise{Promise: &grpcApi.Promise{Id: "foo", State: grpcApi.State_PENDING, Param: &grpcApi.Value{}, Value: &grpcApi.Value{}}}},
						"leaf": {Data: &grpcApi.PromiseOrHref_Promise{Promise: &grpcApi.Promise{Id: "bar", State: grpcApi.State_RESOLVED, Param: &grpcApi.Value{}, Value: &grpcApi.Value{}}}},
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
					Counter:   2,
					ProcessId: "bar",
					Frequency: 1,
				},
			},
			res: &t_api.Response{
				Kind: t_api.ClaimTask,
				ClaimTask: &t_api.ClaimTaskResponse{
					Status:          t_api.StatusCreated,
					Task:            &task.Task{Mesg: &message.Mesg{Type: message.Resume, Root: "foo", Leaf: "bar"}},
					RootPromise:     &promise.Promise{Id: "foo", State: promise.Pending},
					LeafPromise:     &promise.Promise{Id: "bar", State: promise.Resolved},
					RootPromiseHref: "http://localhost:8001/promises/foo",
					LeafPromiseHref: "http://localhost:8001/promises/bar",
				},
			},
		},
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
		},
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
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			grpcTest.Load(t, tc.req, tc.res)

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			var res protoreflect.ProtoMessage
			var err error

			switch req := tc.grpcReq.(type) {
			case *grpcApi.ReadPromiseRequest:
				_, err = grpcTest.promises.ReadPromise(ctx, req)
			case *grpcApi.SearchPromisesRequest:
				_, err = grpcTest.promises.SearchPromises(ctx, req)
			case *grpcApi.CreatePromiseRequest:
				res, err = grpcTest.promises.CreatePromise(ctx, req)
			case *grpcApi.ResolvePromiseRequest:
				res, err = grpcTest.promises.ResolvePromise(ctx, req)
			case *grpcApi.RejectPromiseRequest:
				res, err = grpcTest.promises.RejectPromise(ctx, req)
			case *grpcApi.CancelPromiseRequest:
				res, err = grpcTest.promises.CancelPromise(ctx, req)
			case *grpcApi.CreateCallbackRequest:
				_, err = grpcTest.callbacks.CreateCallback(ctx, req)
			case *grpcApi.ReadScheduleRequest:
				_, err = grpcTest.schedules.ReadSchedule(ctx, req)
			case *grpcApi.SearchSchedulesRequest:
				_, err = grpcTest.schedules.SearchSchedules(ctx, req)
			case *grpcApi.CreateScheduleRequest:
				res, err = grpcTest.schedules.CreateSchedule(ctx, req)
			case *grpcApi.DeleteScheduleRequest:
				_, err = grpcTest.schedules.DeleteSchedule(ctx, req)
			case *grpcApi.AcquireLockRequest:
				_, err = grpcTest.locks.AcquireLock(ctx, req)
			case *grpcApi.ReleaseLockRequest:
				_, err = grpcTest.locks.ReleaseLock(ctx, req)
			case *grpcApi.HeartbeatLocksRequest:
				_, err = grpcTest.locks.HeartbeatLocks(ctx, req)
			case *grpcApi.ClaimTaskRequest:
				res, err = grpcTest.tasks.ClaimTask(ctx, req)
			case *grpcApi.CompleteTaskRequest:
				_, err = grpcTest.tasks.CompleteTask(ctx, req)
			case *grpcApi.HeartbeatTasksRequest:
				_, err = grpcTest.tasks.HeartbeatTasks(ctx, req)
			default:
				t.Fatalf("unexpected type %T", req)
			}

			// assert successful response
			if tc.grpcRes != nil {
				assert.True(t, proto.Equal(tc.grpcRes, res))
			}

			// assert error response
			if err != nil {
				// actual grpc error
				s, ok := status.FromError(err)
				if !ok {
					t.Fatal(err)
				}

				assert.Equal(t, tc.code, s.Code())
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

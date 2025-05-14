package test

import (
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/idempotency"
	"github.com/resonatehq/resonate/pkg/lock"
	"github.com/resonatehq/resonate/pkg/message"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/schedule"
	"github.com/resonatehq/resonate/pkg/task"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/resonatehq/resonate/internal/app/subsystems/api/grpc/pb"
)

type testCase struct {
	Name string
	Req  *t_api.Request
	Res  *t_api.Response
	Http *httpTestCase
	Grpc *grpcTestCase
}

type httpTestCase struct {
	Req *httpTestCaseRequest
	Res *httpTestCaseResponse
}

type httpTestCaseRequest struct {
	Method  string
	Path    string
	Headers map[string]string
	Body    []byte
}

type httpTestCaseResponse struct {
	Code int
	Body []byte
}

type grpcTestCase struct {
	Req  protoreflect.ProtoMessage
	Res  protoreflect.ProtoMessage
	Code codes.Code
}

var TestCases = []*testCase{
	// Promises
	{
		Name: "ReadPromise",
		Req: &t_api.Request{
			Kind: t_api.ReadPromise,
			Tags: map[string]string{"id": "ReadPromise", "name": "ReadPromise"},
			ReadPromise: &t_api.ReadPromiseRequest{
				Id: "foo",
			},
		},
		Res: &t_api.Response{
			Kind: t_api.ReadPromise,
			ReadPromise: &t_api.ReadPromiseResponse{
				Status: t_api.StatusOK,
				Promise: &promise.Promise{
					Id:    "foo",
					State: promise.Pending,
				},
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method:  "GET",
				Path:    "promises/foo",
				Headers: map[string]string{"Request-Id": "ReadPromise"},
			},
			Res: &httpTestCaseResponse{
				Code: 200,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.ReadPromiseRequest{
				Id:        "foo",
				RequestId: "ReadPromise",
			},
		},
	},
	{
		Name: "ReadPromiseWithSlash",
		Req: &t_api.Request{
			Kind: t_api.ReadPromise,
			Tags: map[string]string{"id": "ReadPromiseWithSlash", "name": "ReadPromise"},
			ReadPromise: &t_api.ReadPromiseRequest{
				Id: "foo/bar",
			},
		},
		Res: &t_api.Response{
			Kind: t_api.ReadPromise,
			ReadPromise: &t_api.ReadPromiseResponse{
				Status: t_api.StatusOK,
				Promise: &promise.Promise{
					Id:    "foo/bar",
					State: promise.Pending,
				},
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method:  "GET",
				Path:    "promises/foo/bar",
				Headers: map[string]string{"Request-Id": "ReadPromiseWithSlash"},
			},
			Res: &httpTestCaseResponse{
				Code: 200,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.ReadPromiseRequest{
				Id:        "foo/bar",
				RequestId: "ReadPromiseWithSlash",
			},
		},
	},
	{
		Name: "ReadPromiseNotFound",
		Req: &t_api.Request{
			Kind: t_api.ReadPromise,
			Tags: map[string]string{"id": "ReadPromiseNotFound", "name": "ReadPromise"},
			ReadPromise: &t_api.ReadPromiseRequest{
				Id: "bar",
			},
		},
		Res: &t_api.Response{
			Kind: t_api.ReadPromise,
			ReadPromise: &t_api.ReadPromiseResponse{
				Status:  t_api.StatusPromiseNotFound,
				Promise: nil,
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method:  "GET",
				Path:    "promises/bar",
				Headers: map[string]string{"Request-Id": "ReadPromiseNotFound"},
			},
			Res: &httpTestCaseResponse{
				Code: 404,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.ReadPromiseRequest{
				Id:        "bar",
				RequestId: "ReadPromiseNotFound",
			},
			Code: codes.NotFound,
		},
	},
	{
		Name: "SearchPromises",
		Req: &t_api.Request{
			Kind: t_api.SearchPromises,
			Tags: map[string]string{"id": "SearchPromises", "name": "SearchPromises"},
			SearchPromises: &t_api.SearchPromisesRequest{
				Id: "*",
				States: []promise.State{
					promise.Pending,
					promise.Resolved,
					promise.Rejected,
					promise.Timedout,
					promise.Canceled,
				},
				Tags:  map[string]string{},
				Limit: 10,
			},
		},
		Res: &t_api.Response{
			Kind: t_api.SearchPromises,
			SearchPromises: &t_api.SearchPromisesResponse{
				Status:   t_api.StatusOK,
				Promises: []*promise.Promise{},
				Cursor:   nil,
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method:  "GET",
				Path:    "promises?id=*&limit=10",
				Headers: map[string]string{"Request-Id": "SearchPromises"},
			},
			Res: &httpTestCaseResponse{
				Code: 200,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.SearchPromisesRequest{
				Id:        "*",
				Limit:     10,
				RequestId: "SearchPromises",
			},
		},
	},
	{
		Name: "SearchPromisesCursor",
		Req: &t_api.Request{
			Kind: t_api.SearchPromises,
			Tags: map[string]string{"id": "SearchPromisesCursor", "name": "SearchPromises"},
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
		Res: &t_api.Response{
			Kind: t_api.SearchPromises,
			SearchPromises: &t_api.SearchPromisesResponse{
				Status:   t_api.StatusOK,
				Promises: []*promise.Promise{},
				Cursor:   nil,
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method:  "GET",
				Path:    "promises?cursor=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJOZXh0Ijp7ImlkIjoiKiIsInN0YXRlcyI6WyJQRU5ESU5HIl0sInRhZ3MiOnt9LCJsaW1pdCI6MTAsInNvcnRJZCI6MTAwfX0.XKusWO-Jl4v7QVIwh5Pn3oIElBvtpf0VPOLJkXPvQLk",
				Headers: map[string]string{"Request-Id": "SearchPromisesCursor"},
			},
			Res: &httpTestCaseResponse{
				Code: 200,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.SearchPromisesRequest{
				Id:        "*",
				Cursor:    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJOZXh0Ijp7ImlkIjoiKiIsInN0YXRlcyI6WyJQRU5ESU5HIl0sInRhZ3MiOnt9LCJsaW1pdCI6MTAsInNvcnRJZCI6MTAwfX0.XKusWO-Jl4v7QVIwh5Pn3oIElBvtpf0VPOLJkXPvQLk",
				RequestId: "SearchPromisesCursor",
			},
		},
	},
	{
		Name: "SearchPromisesPending",
		Req: &t_api.Request{
			Kind: t_api.SearchPromises,
			Tags: map[string]string{"id": "SearchPromisesPending", "name": "SearchPromises"},
			SearchPromises: &t_api.SearchPromisesRequest{
				Id: "*",
				States: []promise.State{
					promise.Pending,
				},
				Tags:  map[string]string{},
				Limit: 10,
			},
		},
		Res: &t_api.Response{
			Kind: t_api.SearchPromises,
			SearchPromises: &t_api.SearchPromisesResponse{
				Status:   t_api.StatusOK,
				Promises: []*promise.Promise{},
				Cursor:   nil,
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method:  "GET",
				Path:    "promises?id=*&state=pending&limit=10",
				Headers: map[string]string{"Request-Id": "SearchPromisesPending"},
			},
			Res: &httpTestCaseResponse{
				Code: 200,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.SearchPromisesRequest{
				Id:        "*",
				State:     pb.SearchState_SEARCH_PENDING,
				Limit:     10,
				RequestId: "SearchPromisesPending",
			},
		},
	},
	{
		Name: "SearchPromisesResolved",
		Req: &t_api.Request{
			Kind: t_api.SearchPromises,
			Tags: map[string]string{"id": "SearchPromisesResolved", "name": "SearchPromises"},
			SearchPromises: &t_api.SearchPromisesRequest{
				Id: "*",
				States: []promise.State{
					promise.Resolved,
				},
				Tags:  map[string]string{},
				Limit: 10,
			},
		},
		Res: &t_api.Response{
			Kind: t_api.SearchPromises,
			SearchPromises: &t_api.SearchPromisesResponse{
				Status:   t_api.StatusOK,
				Promises: []*promise.Promise{},
				Cursor:   nil,
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method:  "GET",
				Path:    "promises?id=*&state=resolved&limit=10",
				Headers: map[string]string{"Request-Id": "SearchPromisesResolved"},
			},
			Res: &httpTestCaseResponse{
				Code: 200,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.SearchPromisesRequest{
				Id:        "*",
				State:     pb.SearchState_SEARCH_RESOLVED,
				Limit:     10,
				RequestId: "SearchPromisesResolved",
			},
		},
	},
	{
		Name: "SearchPromisesRejected",
		Req: &t_api.Request{
			Kind: t_api.SearchPromises,
			Tags: map[string]string{"id": "SearchPromisesRejected", "name": "SearchPromises"},
			SearchPromises: &t_api.SearchPromisesRequest{
				Id: "*",
				States: []promise.State{
					promise.Rejected,
					promise.Timedout,
					promise.Canceled,
				},
				Tags:  map[string]string{},
				Limit: 10,
			},
		},
		Res: &t_api.Response{
			Kind: t_api.SearchPromises,
			SearchPromises: &t_api.SearchPromisesResponse{
				Status:   t_api.StatusOK,
				Promises: []*promise.Promise{},
				Cursor:   nil,
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method:  "GET",
				Path:    "promises?id=*&state=rejected&limit=10",
				Headers: map[string]string{"Request-Id": "SearchPromisesRejected"},
			},
			Res: &httpTestCaseResponse{
				Code: 200,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.SearchPromisesRequest{
				Id:        "*",
				State:     pb.SearchState_SEARCH_REJECTED,
				Limit:     10,
				RequestId: "SearchPromisesRejected",
			},
		},
	},
	{
		Name: "SearchPromisesTags",
		Req: &t_api.Request{
			Kind: t_api.SearchPromises,
			Tags: map[string]string{"id": "SearchPromisesTags", "name": "SearchPromises"},
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
					"foo": "bar",
				},
				Limit: 10,
			},
		},
		Res: &t_api.Response{
			Kind: t_api.SearchPromises,
			SearchPromises: &t_api.SearchPromisesResponse{
				Status:   t_api.StatusOK,
				Promises: []*promise.Promise{},
				Cursor:   nil,
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method:  "GET",
				Path:    "promises?id=*&tags[foo]=bar&limit=10",
				Headers: map[string]string{"Request-Id": "SearchPromisesTags"},
			},
			Res: &httpTestCaseResponse{
				Code: 200,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.SearchPromisesRequest{
				Id: "*",
				Tags: map[string]string{
					"foo": "bar",
				},
				Limit:     10,
				RequestId: "SearchPromisesTags",
			},
		},
	},
	{
		Name: "SearchPromisesInvalidQuery",
		Req:  nil,
		Res:  nil,
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "GET",
				Path:   "promises?id=",
			},
			Res: &httpTestCaseResponse{
				Code: 400,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.SearchPromisesRequest{
				Id: "",
			},
			Code: codes.InvalidArgument,
		},
	},
	{
		Name: "SearchPromisesInvalidState",
		Req:  nil,
		Res:  nil,
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "GET",
				Path:   "promises?id=*&limit=10&state=x",
			},
			Res: &httpTestCaseResponse{
				Code: 400,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.SearchPromisesRequest{
				Id:    "*",
				Limit: 10,
				State: -1,
			},
			Code: codes.InvalidArgument,
		},
	},
	{
		Name: "SearchPromisesDefaultLimit",
		Req: &t_api.Request{
			Kind: t_api.SearchPromises,
			Tags: map[string]string{"id": "SearchPromisesDefaultLimit", "name": "SearchPromises"},
			SearchPromises: &t_api.SearchPromisesRequest{
				Id: "*",
				States: []promise.State{
					promise.Pending,
					promise.Resolved,
					promise.Rejected,
					promise.Timedout,
					promise.Canceled,
				},
				Tags:  map[string]string{},
				Limit: 100,
			},
		},
		Res: &t_api.Response{
			Kind: t_api.SearchPromises,
			SearchPromises: &t_api.SearchPromisesResponse{
				Status:   t_api.StatusOK,
				Promises: []*promise.Promise{},
				Cursor:   nil,
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method:  "GET",
				Path:    "promises?id=*",
				Headers: map[string]string{"Request-Id": "SearchPromisesDefaultLimit"},
			},
			Res: &httpTestCaseResponse{
				Code: 200,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.SearchPromisesRequest{
				Id:        "*",
				RequestId: "SearchPromisesDefaultLimit",
			},
		},
	},
	{
		Name: "SearchPromisesInvalidLimitLower",
		Req:  nil,
		Res:  nil,
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "GET",
				Path:   "promises?id=*&limit=-1",
			},
			Res: &httpTestCaseResponse{
				Code: 400,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.SearchPromisesRequest{
				Id:    "*",
				Limit: -1,
			},
			Code: codes.InvalidArgument,
		},
	},
	{
		Name: "SearchPromisesInvalidLimitUpper",
		Req:  nil,
		Res:  nil,
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "GET",
				Path:   "promises?id=*&limit=101",
			},
			Res: &httpTestCaseResponse{
				Code: 400,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.SearchPromisesRequest{
				Id:    "*",
				Limit: 101,
			},
			Code: codes.InvalidArgument,
		},
	},
	{
		Name: "CreatePromise",
		Req: &t_api.Request{
			Kind: t_api.CreatePromise,
			Tags: map[string]string{"id": "CreatePromise", "name": "CreatePromise"},
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
		Res: &t_api.Response{
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
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "POST",
				Path:   "promises",
				Headers: map[string]string{
					"Request-Id":      "CreatePromise",
					"Idempotency-Key": "bar",
					"Strict":          "true",
				},
				Body: []byte(`{
					"id": "foo",
					"param": {
						"headers": {"a":"a","b":"b","c":"c"},
						"data": "cGVuZGluZw=="
					},
					"timeout": 1
				}`),
			},
			Res: &httpTestCaseResponse{
				Code: 201,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.CreatePromiseRequest{
				Id:             "foo",
				IdempotencyKey: "bar",
				Strict:         true,
				Param: &pb.Value{
					Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
					Data:    []byte("pending"),
				},
				Timeout:   1,
				RequestId: "CreatePromise",
			},
			Res: &pb.CreatePromiseResponse{
				Noop: false,
				Promise: &pb.Promise{
					Id:                      "foo",
					State:                   pb.State_PENDING,
					IdempotencyKeyForCreate: "bar",
					Param: &pb.Value{
						Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
						Data:    []byte("pending"),
					},
					Value:   &pb.Value{},
					Timeout: 1,
				},
			},
		},
	},
	{
		Name: "CreatePromiseMinimal",
		Req: &t_api.Request{
			Kind: t_api.CreatePromise,
			Tags: map[string]string{"id": "CreatePromiseMinimal", "name": "CreatePromise"},
			CreatePromise: &t_api.CreatePromiseRequest{
				Id:             "foo",
				IdempotencyKey: nil,
				Strict:         false,
				Param:          promise.Value{},
				Timeout:        1,
			},
		},
		Res: &t_api.Response{
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
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "POST",
				Path:   "promises",
				Headers: map[string]string{
					"Request-Id": "CreatePromiseMinimal",
				},
				Body: []byte(`{
					"id": "foo",
					"timeout": 1
				}`),
			},
			Res: &httpTestCaseResponse{
				Code: 201,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.CreatePromiseRequest{
				Id:        "foo",
				Timeout:   1,
				RequestId: "CreatePromiseMinimal",
			},
			Res: &pb.CreatePromiseResponse{
				Noop: false,
				Promise: &pb.Promise{
					Id:                      "foo",
					State:                   pb.State_PENDING,
					IdempotencyKeyForCreate: "",
					Param:                   &pb.Value{},
					Value:                   &pb.Value{},
					Timeout:                 1,
				},
			},
		},
	},
	{
		Name: "CreatePromiseAndTask",
		Req: &t_api.Request{
			Kind: t_api.CreatePromiseAndTask,
			Tags: map[string]string{"id": "CreatePromiseAndTask", "name": "CreatePromiseAndTask"},
			CreatePromiseAndTask: &t_api.CreatePromiseAndTaskRequest{
				Promise: &t_api.CreatePromiseRequest{
					Id:      "foo",
					Timeout: 1,
					Tags:    map[string]string{"resonate:invoke": "baz"},
				},
				Task: &t_api.CreateTaskRequest{
					PromiseId: "foo",
					ProcessId: "bar",
					Ttl:       2,
					Timeout:   1,
				},
			},
		},
		Res: &t_api.Response{
			Kind: t_api.CreatePromiseAndTask,
			CreatePromiseAndTask: &t_api.CreatePromiseAndTaskResponse{
				Status: t_api.StatusCreated,
				Promise: &promise.Promise{
					Id:      "foo",
					State:   promise.Pending,
					Param:   promise.Value{},
					Timeout: 1,
				},
				Task: &task.Task{},
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "POST",
				Path:   "promises/task",
				Headers: map[string]string{
					"Request-Id": "CreatePromiseAndTask",
				},
				Body: []byte(`{
					"promise": {"id": "foo", "timeout": 1, "tags": {"resonate:invoke": "baz"}},
					"task": {"processId": "bar", "ttl": 2}
				}`),
			},
			Res: &httpTestCaseResponse{
				Code: 201,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.CreatePromiseAndTaskRequest{
				Promise: &pb.CreatePromiseRequest{
					Id:        "foo",
					Timeout:   1,
					RequestId: "CreatePromiseAndTask",
					Tags:      map[string]string{"resonate:invoke": "baz"},
				},
				Task: &pb.CreatePromiseTaskRequest{
					ProcessId: "bar",
					Ttl:       2,
				},
			},
			Res: &pb.CreatePromiseAndTaskResponse{
				Noop: false,
				Promise: &pb.Promise{
					Id:                      "foo",
					State:                   pb.State_PENDING,
					IdempotencyKeyForCreate: "",
					Param:                   &pb.Value{},
					Value:                   &pb.Value{},
					Timeout:                 1,
				},
			},
		},
	},
	{
		Name: "ResolvePromise",
		Req: &t_api.Request{
			Kind: t_api.CompletePromise,
			Tags: map[string]string{"id": "ResolvePromise", "name": "CompletePromise"},
			CompletePromise: &t_api.CompletePromiseRequest{
				Id:             "foo",
				IdempotencyKey: util.ToPointer(idempotency.Key("bar")),
				Strict:         true,
				State:          promise.Resolved,
				Value: promise.Value{
					Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
					Data:    []byte("resolve"),
				},
			},
		},
		Res: &t_api.Response{
			Kind: t_api.CompletePromise,
			CompletePromise: &t_api.CompletePromiseResponse{
				Status: t_api.StatusCreated,
				Promise: &promise.Promise{
					Id:                        "foo",
					State:                     promise.Resolved,
					IdempotencyKeyForComplete: util.ToPointer(idempotency.Key("bar")),
					Value: promise.Value{
						Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
						Data:    []byte("resolve"),
					},
				},
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "PATCH",
				Path:   "promises/foo",
				Headers: map[string]string{
					"Request-Id":      "ResolvePromise",
					"Idempotency-Key": "bar",
					"Strict":          "true",
				},
				Body: []byte(`{
					"state": "RESOLVED",
					"value": {
						"headers": {"a":"a","b":"b","c":"c"},
						"data": "cmVzb2x2ZQ=="
					}
				}`),
			},
			Res: &httpTestCaseResponse{
				Code: 201,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.ResolvePromiseRequest{
				RequestId:      "ResolvePromise",
				Id:             "foo",
				IdempotencyKey: "bar",
				Strict:         true,
				Value: &pb.Value{
					Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
					Data:    []byte("resolve"),
				},
			},
			Res: &pb.ResolvePromiseResponse{
				Noop: false,
				Promise: &pb.Promise{
					Id:                        "foo",
					State:                     pb.State_RESOLVED,
					IdempotencyKeyForComplete: "bar",
					Param:                     &pb.Value{},
					Value: &pb.Value{
						Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
						Data:    []byte("resolve"),
					},
				},
			},
		},
	},
	{
		Name: "ResolvePromiseMinimal",
		Req: &t_api.Request{
			Kind: t_api.CompletePromise,
			Tags: map[string]string{"id": "ResolvePromiseMinimal", "name": "CompletePromise"},
			CompletePromise: &t_api.CompletePromiseRequest{
				Id:             "foo",
				IdempotencyKey: nil,
				Strict:         false,
				State:          promise.Resolved,
				Value:          promise.Value{},
			},
		},
		Res: &t_api.Response{
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
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "PATCH",
				Path:   "promises/foo",
				Headers: map[string]string{
					"Request-Id": "ResolvePromiseMinimal",
				},
				Body: []byte(`{
					"state": "RESOLVED"
				}`),
			},
			Res: &httpTestCaseResponse{
				Code: 201,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.ResolvePromiseRequest{
				Id:        "foo",
				RequestId: "ResolvePromiseMinimal",
			},
			Res: &pb.ResolvePromiseResponse{
				Noop: false,
				Promise: &pb.Promise{
					Id:                        "foo",
					State:                     pb.State_RESOLVED,
					IdempotencyKeyForComplete: "",
					Param:                     &pb.Value{},
					Value:                     &pb.Value{},
				},
			},
		},
	},
	{
		Name: "ResolvePromiseAlreadyCompleted",
		Req: &t_api.Request{
			Kind: t_api.CompletePromise,
			Tags: map[string]string{"id": "ResolvePromiseAlreadyCompleted", "name": "CompletePromise"},
			CompletePromise: &t_api.CompletePromiseRequest{
				Id:             "foo",
				IdempotencyKey: nil,
				Strict:         false,
				State:          promise.Resolved,
				Value:          promise.Value{},
			},
		},
		Res: &t_api.Response{
			Kind: t_api.CompletePromise,
			CompletePromise: &t_api.CompletePromiseResponse{
				Status: t_api.StatusPromiseAlreadyResolved,
				Promise: &promise.Promise{
					Id:    "foo",
					State: promise.Resolved,
				},
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "PATCH",
				Path:   "promises/foo",
				Headers: map[string]string{
					"Request-Id": "ResolvePromiseAlreadyCompleted",
				},
				Body: []byte(`{
					"state": "RESOLVED"
				}`),
			},
			Res: &httpTestCaseResponse{
				Code: 403,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.ResolvePromiseRequest{
				Id:        "foo",
				RequestId: "ResolvePromiseAlreadyCompleted",
			},
			Code: codes.PermissionDenied,
		},
	},
	{
		Name: "RejectPromise",
		Req: &t_api.Request{
			Kind: t_api.CompletePromise,
			Tags: map[string]string{"id": "RejectPromise", "name": "CompletePromise"},
			CompletePromise: &t_api.CompletePromiseRequest{
				Id:             "foo",
				IdempotencyKey: util.ToPointer(idempotency.Key("bar")),
				Strict:         true,
				State:          promise.Rejected,
				Value: promise.Value{
					Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
					Data:    []byte("reject"),
				},
			},
		},
		Res: &t_api.Response{
			Kind: t_api.CompletePromise,
			CompletePromise: &t_api.CompletePromiseResponse{
				Status: t_api.StatusCreated,
				Promise: &promise.Promise{
					Id:                        "foo",
					State:                     promise.Rejected,
					IdempotencyKeyForComplete: util.ToPointer(idempotency.Key("bar")),
					Value: promise.Value{
						Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
						Data:    []byte("reject"),
					},
				},
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "PATCH",
				Path:   "promises/foo",
				Headers: map[string]string{
					"Request-Id":      "RejectPromise",
					"Idempotency-Key": "bar",
					"Strict":          "true",
				},
				Body: []byte(`{
					"state": "REJECTED",
					"value": {
						"headers": {"a":"a","b":"b","c":"c"},
						"data": "cmVqZWN0"
					}
				}`),
			},
			Res: &httpTestCaseResponse{
				Code: 201,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.RejectPromiseRequest{
				RequestId:      "RejectPromise",
				Id:             "foo",
				IdempotencyKey: "bar",
				Strict:         true,
				Value: &pb.Value{
					Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
					Data:    []byte("reject"),
				},
			},
			Res: &pb.RejectPromiseResponse{
				Noop: false,
				Promise: &pb.Promise{
					Id:                        "foo",
					State:                     pb.State_REJECTED,
					IdempotencyKeyForComplete: "bar",
					Param:                     &pb.Value{},
					Value: &pb.Value{
						Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
						Data:    []byte("reject"),
					},
				},
			},
		},
	},
	{
		Name: "RejectPromiseMinimal",
		Req: &t_api.Request{
			Kind: t_api.CompletePromise,
			Tags: map[string]string{"id": "RejectPromiseMinimal", "name": "CompletePromise"},
			CompletePromise: &t_api.CompletePromiseRequest{
				Id:             "foo",
				IdempotencyKey: nil,
				Strict:         false,
				State:          promise.Rejected,
				Value:          promise.Value{},
			},
		},
		Res: &t_api.Response{
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
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "PATCH",
				Path:   "promises/foo",
				Headers: map[string]string{
					"Request-Id": "RejectPromiseMinimal",
				},
				Body: []byte(`{
					"state": "REJECTED"
				}`),
			},
			Res: &httpTestCaseResponse{
				Code: 201,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.RejectPromiseRequest{
				Id:        "foo",
				RequestId: "RejectPromiseMinimal",
			},
			Res: &pb.RejectPromiseResponse{
				Noop: false,
				Promise: &pb.Promise{
					Id:                        "foo",
					State:                     pb.State_REJECTED,
					IdempotencyKeyForComplete: "",
					Param:                     &pb.Value{},
					Value:                     &pb.Value{},
				},
			},
		},
	},
	{
		Name: "RejectPromiseAlreadyCompleted",
		Req: &t_api.Request{
			Kind: t_api.CompletePromise,
			Tags: map[string]string{"id": "RejectPromiseAlreadyCompleted", "name": "CompletePromise"},
			CompletePromise: &t_api.CompletePromiseRequest{
				Id:             "foo",
				IdempotencyKey: nil,
				Strict:         false,
				State:          promise.Rejected,
				Value:          promise.Value{},
			},
		},
		Res: &t_api.Response{
			Kind: t_api.CompletePromise,
			CompletePromise: &t_api.CompletePromiseResponse{
				Status: t_api.StatusPromiseAlreadyRejected,
				Promise: &promise.Promise{
					Id:    "foo",
					State: promise.Rejected,
				},
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "PATCH",
				Path:   "promises/foo",
				Headers: map[string]string{
					"Request-Id": "RejectPromiseAlreadyCompleted",
				},
				Body: []byte(`{
					"state": "REJECTED"
				}`),
			},
			Res: &httpTestCaseResponse{
				Code: 403,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.RejectPromiseRequest{
				Id:        "foo",
				RequestId: "RejectPromiseAlreadyCompleted",
			},
			Code: codes.PermissionDenied,
		},
	},
	{
		Name: "CancelPromise",
		Req: &t_api.Request{
			Kind: t_api.CompletePromise,
			Tags: map[string]string{"id": "CancelPromise", "name": "CompletePromise"},
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
		Res: &t_api.Response{
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
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "PATCH",
				Path:   "promises/foo",
				Headers: map[string]string{
					"Request-Id":      "CancelPromise",
					"Idempotency-Key": "bar",
					"Strict":          "true",
				},
				Body: []byte(`{
					"state": "REJECTED_CANCELED",
					"value": {
						"headers": {"a":"a","b":"b","c":"c"},
						"data": "Y2FuY2Vs"
					}
				}`),
			},
			Res: &httpTestCaseResponse{
				Code: 201,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.CancelPromiseRequest{
				Id:             "foo",
				IdempotencyKey: "bar",
				Strict:         true,
				Value: &pb.Value{
					Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
					Data:    []byte("cancel"),
				},
				RequestId: "CancelPromise",
			},
			Res: &pb.CancelPromiseResponse{
				Noop: false,
				Promise: &pb.Promise{
					Id:                        "foo",
					State:                     pb.State_REJECTED_CANCELED,
					IdempotencyKeyForComplete: "bar",
					Param:                     &pb.Value{},
					Value: &pb.Value{
						Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
						Data:    []byte("cancel"),
					},
				},
			},
		},
	},
	{
		Name: "CancelPromiseMinimal",
		Req: &t_api.Request{
			Kind: t_api.CompletePromise,
			Tags: map[string]string{"id": "CancelPromiseMinimal", "name": "CompletePromise"},
			CompletePromise: &t_api.CompletePromiseRequest{
				Id:             "foo",
				IdempotencyKey: nil,
				Strict:         false,
				State:          promise.Canceled,
				Value:          promise.Value{},
			},
		},
		Res: &t_api.Response{
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
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "PATCH",
				Path:   "promises/foo",
				Headers: map[string]string{
					"Request-Id": "CancelPromiseMinimal",
				},
				Body: []byte(`{
					"state": "REJECTED_CANCELED"
				}`),
			},
			Res: &httpTestCaseResponse{
				Code: 201,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.CancelPromiseRequest{
				RequestId: "CancelPromiseMinimal",
				Id:        "foo",
			},
			Res: &pb.CancelPromiseResponse{
				Noop: false,
				Promise: &pb.Promise{
					Id:                        "foo",
					State:                     pb.State_REJECTED_CANCELED,
					IdempotencyKeyForComplete: "",
					Param:                     &pb.Value{},
					Value:                     &pb.Value{},
				},
			},
		},
	},
	{
		Name: "CancelPromiseAlreadyCompleted",
		Req: &t_api.Request{
			Kind: t_api.CompletePromise,
			Tags: map[string]string{"id": "CancelPromiseAlreadyCompleted", "name": "CompletePromise"},
			CompletePromise: &t_api.CompletePromiseRequest{
				Id:             "foo",
				IdempotencyKey: nil,
				Strict:         false,
				State:          promise.Canceled,
				Value:          promise.Value{},
			},
		},
		Res: &t_api.Response{
			Kind: t_api.CompletePromise,
			CompletePromise: &t_api.CompletePromiseResponse{
				Status: t_api.StatusPromiseAlreadyRejected,
				Promise: &promise.Promise{
					Id:    "foo",
					State: promise.Canceled,
				},
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "PATCH",
				Path:   "promises/foo",
				Headers: map[string]string{
					"Request-Id": "CancelPromiseAlreadyCompleted",
				},
				Body: []byte(`{
					"state": "REJECTED_CANCELED"
				}`),
			},
			Res: &httpTestCaseResponse{
				Code: 403,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.CancelPromiseRequest{
				Id:        "foo",
				RequestId: "CancelPromiseAlreadyCompleted",
			},
			Code: codes.PermissionDenied,
		},
	},

	// Callbacks
	{
		Name: "CreateCallbackLogicalReceiver",
		Req: &t_api.Request{
			Kind: t_api.CreateCallback,
			Tags: map[string]string{"id": "CreateCallback", "name": "CreateCallback"},
			CreateCallback: &t_api.CreateCallbackRequest{
				Id:        "__resume:bar:foo",
				PromiseId: "foo",
				Recv:      []byte(`"foo"`),
				Mesg:      &message.Mesg{Type: "resume", Root: "bar", Leaf: "foo"},
				Timeout:   1,
			},
		},
		Res: &t_api.Response{
			Kind: t_api.CreateCallback,
			CreateCallback: &t_api.CreateCallbackResponse{
				Status: t_api.StatusCreated,
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "POST",
				Path:   "callbacks",
				Headers: map[string]string{
					"Request-Id": "CreateCallback",
				},
				Body: []byte(`{
					"id": "foo.1",
					"promiseId": "foo",
					"rootPromiseId": "bar",
					"timeout": 1,
					"recv": "foo"
				}`),
			},
			Res: &httpTestCaseResponse{
				Code: 201,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.CreateCallbackRequest{
				PromiseId:     "foo",
				RootPromiseId: "bar",
				Timeout:       1,
				Recv:          &pb.Recv{Recv: &pb.Recv_Logical{Logical: "foo"}},
				RequestId:     "CreateCallback",
			},
		},
	},
	{
		Name: "CreateCallbackPhysicalReceiver",
		Req: &t_api.Request{
			Kind: t_api.CreateCallback,
			Tags: map[string]string{"id": "CreateCallbackPhysicalReceiver", "name": "CreateCallback"},
			CreateCallback: &t_api.CreateCallbackRequest{
				Id:        "__resume:bar:foo",
				PromiseId: "foo",
				Recv:      []byte(`{"type":"http","data":{"url":"http://localhost:3000"}}`),
				Mesg:      &message.Mesg{Type: "resume", Root: "bar", Leaf: "foo"},
				Timeout:   1,
			},
		},
		Res: &t_api.Response{
			Kind: t_api.CreateCallback,
			CreateCallback: &t_api.CreateCallbackResponse{
				Status: t_api.StatusCreated,
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "POST",
				Path:   "callbacks",
				Headers: map[string]string{
					"Request-Id": "CreateCallbackPhysicalReceiver",
				},
				Body: []byte(`{
					"id": "foo.1",
					"promiseId": "foo",
					"rootPromiseId": "bar",
					"timeout": 1,
					"recv": {"type":"http","data":{"url":"http://localhost:3000"}}
				}`),
			},
			Res: &httpTestCaseResponse{
				Code: 201,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.CreateCallbackRequest{
				PromiseId:     "foo",
				RootPromiseId: "bar",
				Timeout:       1,
				Recv:          &pb.Recv{Recv: &pb.Recv_Physical{Physical: &pb.PhysicalRecv{Type: "http", Data: []byte(`{"url":"http://localhost:3000"}`)}}},
				RequestId:     "CreateCallbackPhysicalReceiver",
			},
		},
	},
	{
		Name: "CreateCallbackNotFound",
		Req: &t_api.Request{
			Kind: t_api.CreateCallback,
			Tags: map[string]string{"id": "CreateCallbackNotFound", "name": "CreateCallback"},
			CreateCallback: &t_api.CreateCallbackRequest{
				Id:        "__resume:bar:foo",
				PromiseId: "foo",
				Recv:      []byte(`"foo"`),
				Mesg:      &message.Mesg{Type: "resume", Root: "bar", Leaf: "foo"},
				Timeout:   1,
			},
		},
		Res: &t_api.Response{
			Kind: t_api.CreateCallback,
			CreateCallback: &t_api.CreateCallbackResponse{
				Status: t_api.StatusPromiseNotFound,
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "POST",
				Path:   "callbacks",
				Headers: map[string]string{
					"Request-Id": "CreateCallbackNotFound",
				},
				Body: []byte(`{
					"id": "foo.1",
					"promiseId": "foo",
					"rootPromiseId": "bar",
					"timeout": 1,
					"recv": "foo"
				}`),
			},
			Res: &httpTestCaseResponse{
				Code: 404,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.CreateCallbackRequest{
				PromiseId:     "foo",
				RootPromiseId: "bar",
				Timeout:       1,
				Recv:          &pb.Recv{Recv: &pb.Recv_Logical{Logical: "foo"}},
				RequestId:     "CreateCallbackNotFound",
			},
			Code: codes.NotFound,
		},
	},

	// Subscriptions
	{
		Name: "CreateSubscriptionLogicalReceiver",
		Req: &t_api.Request{
			Kind: t_api.CreateCallback,
			Tags: map[string]string{"id": "CreateSubscription", "name": "CreateCallback"},
			CreateCallback: &t_api.CreateCallbackRequest{
				Id:        "__notify:foo:foo.1",
				PromiseId: "foo",
				Recv:      []byte(`"foo"`),
				Mesg:      &message.Mesg{Type: "notify", Root: "foo"},
				Timeout:   1,
			},
		},
		Res: &t_api.Response{
			Kind: t_api.CreateCallback,
			CreateCallback: &t_api.CreateCallbackResponse{
				Status: t_api.StatusCreated,
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "POST",
				Path:   "subscriptions",
				Headers: map[string]string{
					"Request-Id": "CreateSubscription",
				},
				Body: []byte(`{
					"id": "foo.1",
					"promiseId": "foo",
					"timeout": 1,
					"recv": "foo"
				}`),
			},
			Res: &httpTestCaseResponse{
				Code: 201,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.CreateSubscriptionRequest{
				Id:        "foo.1",
				PromiseId: "foo",
				Timeout:   1,
				Recv:      &pb.Recv{Recv: &pb.Recv_Logical{Logical: "foo"}},
				RequestId: "CreateSubscription",
			},
		},
	},
	{
		Name: "CreateSubscriptionPhysicalReceiver",
		Req: &t_api.Request{
			Kind: t_api.CreateCallback,
			Tags: map[string]string{"id": "CreateSubscriptionPhysicalRecv", "name": "CreateCallback"},
			CreateCallback: &t_api.CreateCallbackRequest{
				Id:        "__notify:foo:foo.1",
				PromiseId: "foo",
				Recv:      []byte(`{"type":"http","data":{"url":"http://localhost:3000"}}`),
				Mesg:      &message.Mesg{Type: "notify", Root: "foo"},
				Timeout:   1,
			},
		},
		Res: &t_api.Response{
			Kind: t_api.CreateCallback,
			CreateCallback: &t_api.CreateCallbackResponse{
				Status: t_api.StatusCreated,
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "POST",
				Path:   "subscriptions",
				Headers: map[string]string{
					"Request-Id": "CreateSubscriptionPhysicalRecv",
				},
				Body: []byte(`{
					"id": "foo.1",
					"promiseId": "foo",
					"timeout": 1,
					"recv": {"type":"http","data":{"url":"http://localhost:3000"}}
				}`),
			},
			Res: &httpTestCaseResponse{
				Code: 201,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.CreateSubscriptionRequest{
				Id:        "foo.1",
				PromiseId: "foo",
				Timeout:   1,
				Recv:      &pb.Recv{Recv: &pb.Recv_Physical{Physical: &pb.PhysicalRecv{Type: "http", Data: []byte(`{"url":"http://localhost:3000"}`)}}},
				RequestId: "CreateSubscriptionPhysicalRecv",
			},
		},
	},
	{
		Name: "CreateSubscriptionNotFound",
		Req: &t_api.Request{
			Kind: t_api.CreateCallback,
			Tags: map[string]string{"id": "CreateSubscriptionNotFound", "name": "CreateCallback"},
			CreateCallback: &t_api.CreateCallbackRequest{
				Id:        "__notify:foo:foo.1",
				PromiseId: "foo",
				Recv:      []byte(`"foo"`),
				Mesg:      &message.Mesg{Type: "notify", Root: "foo"},
				Timeout:   1,
			},
		},
		Res: &t_api.Response{
			Kind: t_api.CreateCallback,
			CreateCallback: &t_api.CreateCallbackResponse{
				Status: t_api.StatusPromiseNotFound,
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "POST",
				Path:   "subscriptions",
				Headers: map[string]string{
					"Request-Id": "CreateSubscriptionNotFound",
				},
				Body: []byte(`{
					"id": "foo.1",
					"promiseId": "foo",
					"timeout": 1,
					"recv": "foo"
				}`),
			},
			Res: &httpTestCaseResponse{
				Code: 404,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.CreateSubscriptionRequest{
				Id:        "foo.1",
				PromiseId: "foo",
				Timeout:   1,
				Recv:      &pb.Recv{Recv: &pb.Recv_Logical{Logical: "foo"}},
				RequestId: "CreateSubscriptionNotFound",
			},
			Code: codes.NotFound,
		},
	},
	// Schedules
	{
		Name: "ReadSchedule",
		Req: &t_api.Request{
			Kind: t_api.ReadSchedule,
			Tags: map[string]string{"id": "ReadSchedule", "name": "ReadSchedule"},
			ReadSchedule: &t_api.ReadScheduleRequest{
				Id: "foo",
			},
		},
		Res: &t_api.Response{
			Kind: t_api.ReadSchedule,
			ReadSchedule: &t_api.ReadScheduleResponse{
				Status: t_api.StatusOK,
				Schedule: &schedule.Schedule{
					Id:             "foo",
					Description:    "",
					Cron:           "* * * * * *",
					PromiseId:      "foo.{{.timestamp}}",
					PromiseTimeout: 1000000,
				},
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "GET",
				Path:   "schedules/foo",
				Headers: map[string]string{
					"Request-Id": "ReadSchedule",
				},
			},
			Res: &httpTestCaseResponse{
				Code: 200,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.ReadScheduleRequest{
				Id:        "foo",
				RequestId: "ReadSchedule",
			},
		},
	},
	{
		Name: "SearchSchedules",
		Req: &t_api.Request{
			Kind: t_api.SearchSchedules,
			Tags: map[string]string{"id": "SearchSchedules", "name": "SearchSchedules"},
			SearchSchedules: &t_api.SearchSchedulesRequest{
				Id:    "*",
				Tags:  map[string]string{},
				Limit: 10,
			},
		},
		Res: &t_api.Response{
			Kind: t_api.SearchSchedules,
			SearchSchedules: &t_api.SearchSchedulesResponse{
				Status:    t_api.StatusOK,
				Schedules: []*schedule.Schedule{},
				Cursor:    nil,
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "GET",
				Path:   "schedules?id=*&limit=10",
				Headers: map[string]string{
					"Request-Id": "SearchSchedules",
				},
			},
			Res: &httpTestCaseResponse{
				Code: 200,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.SearchSchedulesRequest{
				RequestId: "SearchSchedules",
				Id:        "*",
				Limit:     10,
			},
		},
	},
	{
		Name: "SearchSchedulesCursor",
		Req: &t_api.Request{
			Kind: t_api.SearchSchedules,
			Tags: map[string]string{"id": "SearchSchedulesCursor", "name": "SearchSchedules"},
			SearchSchedules: &t_api.SearchSchedulesRequest{
				Id:     "*",
				Tags:   map[string]string{},
				Limit:  10,
				SortId: util.ToPointer(int64(100)),
			},
		},
		Res: &t_api.Response{
			Kind: t_api.SearchSchedules,
			SearchSchedules: &t_api.SearchSchedulesResponse{
				Status:    t_api.StatusOK,
				Schedules: []*schedule.Schedule{},
				Cursor:    nil,
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method:  "GET",
				Path:    "schedules?cursor=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJOZXh0Ijp7ImlkIjoiKiIsInRhZ3MiOnt9LCJsaW1pdCI6MTAsInNvcnRJZCI6MTAwfX0.w5_elkl3n5yUHKIbxBzdA1sWRxvKLGVqsnz-H69p_JI",
				Headers: map[string]string{"Request-Id": "SearchSchedulesCursor"},
			},
			Res: &httpTestCaseResponse{
				Code: 200,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.SearchSchedulesRequest{
				Id:        "*",
				Cursor:    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJOZXh0Ijp7ImlkIjoiKiIsInRhZ3MiOnt9LCJsaW1pdCI6MTAsInNvcnRJZCI6MTAwfX0.w5_elkl3n5yUHKIbxBzdA1sWRxvKLGVqsnz-H69p_JI",
				RequestId: "SearchSchedulesCursor",
			},
		},
	},
	{
		Name: "SearchSchedulesTags",
		Req: &t_api.Request{
			Kind: t_api.SearchSchedules,
			Tags: map[string]string{"id": "SearchSchedulesTags", "name": "SearchSchedules"},
			SearchSchedules: &t_api.SearchSchedulesRequest{
				Id: "*",
				Tags: map[string]string{
					"foo": "bar",
				},
				Limit: 10,
			},
		},
		Res: &t_api.Response{
			Kind: t_api.SearchSchedules,
			SearchSchedules: &t_api.SearchSchedulesResponse{
				Status:    t_api.StatusOK,
				Schedules: []*schedule.Schedule{},
				Cursor:    nil,
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "GET",
				Path:   "schedules?id=*&tags[foo]=bar&limit=10",
				Headers: map[string]string{
					"Request-Id": "SearchSchedulesTags",
				},
			},
			Res: &httpTestCaseResponse{
				Code: 200,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.SearchSchedulesRequest{
				Id: "*",
				Tags: map[string]string{
					"foo": "bar",
				},
				Limit:     10,
				RequestId: "SearchSchedulesTags",
			},
		},
	},
	{
		Name: "SearchSchedulesInvalidQuery",
		Req:  nil,
		Res:  nil,
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "GET",
				Path:   "schedules?id=",
			},
			Res: &httpTestCaseResponse{
				Code: 400,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.SearchSchedulesRequest{
				Id: "",
			},
			Code: codes.InvalidArgument,
		},
	},
	{
		Name: "SearchSchedulesDefaultLimit",
		Req: &t_api.Request{
			Kind: t_api.SearchSchedules,
			Tags: map[string]string{"id": "SearchSchedulesDefaultLimit", "name": "SearchSchedules"},
			SearchSchedules: &t_api.SearchSchedulesRequest{
				Id:    "*",
				Tags:  map[string]string{},
				Limit: 100,
			},
		},
		Res: &t_api.Response{
			Kind: t_api.SearchSchedules,
			SearchSchedules: &t_api.SearchSchedulesResponse{
				Status:    t_api.StatusOK,
				Schedules: []*schedule.Schedule{},
				Cursor:    nil,
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "GET",
				Path:   "schedules?id=*",
				Headers: map[string]string{
					"Request-Id": "SearchSchedulesDefaultLimit",
				},
			},
			Res: &httpTestCaseResponse{
				Code: 200,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.SearchSchedulesRequest{
				Id:        "*",
				RequestId: "SearchSchedulesDefaultLimit",
			},
		},
	},
	{
		Name: "SearchSchedulesInvalidLimitLower",
		Req:  nil,
		Res:  nil,
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "GET",
				Path:   "schedules?id=*&limit=-1",
			},
			Res: &httpTestCaseResponse{
				Code: 400,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.SearchSchedulesRequest{
				Id:    "*",
				Limit: -1,
			},
			Code: codes.InvalidArgument,
		},
	},
	{
		Name: "SearchSchedulesInvalidLimitUpper",
		Req:  nil,
		Res:  nil,
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "GET",
				Path:   "schedules?id=*&limit=101",
			},
			Res: &httpTestCaseResponse{
				Code: 400,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.SearchSchedulesRequest{
				Id:    "*",
				Limit: 101,
			},
			Code: codes.InvalidArgument,
		},
	},
	{
		Name: "CreateSchedule",
		Req: &t_api.Request{
			Kind: t_api.CreateSchedule,
			Tags: map[string]string{"id": "CreateSchedule", "name": "CreateSchedule"},
			CreateSchedule: &t_api.CreateScheduleRequest{
				Id:             "foo",
				IdempotencyKey: util.ToPointer(idempotency.Key("bar")),
				Cron:           "* * * * *",
				PromiseId:      "foo.{{.timestamp}}",
				PromiseTimeout: 1,
			},
		},
		Res: &t_api.Response{
			Kind: t_api.CreateSchedule,
			CreateSchedule: &t_api.CreateScheduleResponse{
				Status: t_api.StatusCreated,
				Schedule: &schedule.Schedule{
					Id:             "foo",
					Description:    "",
					Cron:           "* * * * *",
					PromiseId:      "foo.{{.timestamp}}",
					PromiseTimeout: 1,
					IdempotencyKey: util.ToPointer(idempotency.Key("bar")),
				},
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "POST",
				Path:   "schedules",
				Headers: map[string]string{
					"Request-Id":      "CreateSchedule",
					"Idempotency-Key": "bar",
				},
				Body: []byte(`{
					"id": "foo",
					"desc": "",
					"cron": "* * * * *",
					"promiseId": "foo.{{.timestamp}}",
					"promiseTimeout": 1
				}`),
			},
			Res: &httpTestCaseResponse{
				Code: 201,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.CreateScheduleRequest{
				Id:             "foo",
				Description:    "",
				Cron:           "* * * * *",
				PromiseId:      "foo.{{.timestamp}}",
				PromiseTimeout: 1,
				IdempotencyKey: "bar",
				RequestId:      "CreateSchedule",
			},
			Res: &pb.CreatedScheduleResponse{
				Noop: false,
				Schedule: &pb.Schedule{
					Id:             "foo",
					Description:    "",
					Cron:           "* * * * *",
					PromiseId:      "foo.{{.timestamp}}",
					PromiseTimeout: 1,
					PromiseParam:   &pb.Value{},
					IdempotencyKey: "bar",
				},
			},
		},
	},
	{
		Name: "DeleteSchedule",
		Req: &t_api.Request{
			Kind: t_api.DeleteSchedule,
			Tags: map[string]string{"id": "DeleteSchedule", "name": "DeleteSchedule"},
			DeleteSchedule: &t_api.DeleteScheduleRequest{
				Id: "foo",
			},
		},
		Res: &t_api.Response{
			Kind: t_api.DeleteSchedule,
			DeleteSchedule: &t_api.DeleteScheduleResponse{
				Status: t_api.StatusNoContent,
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "DELETE",
				Path:   "schedules/foo",
				Headers: map[string]string{
					"Request-Id": "DeleteSchedule",
				},
			},
			Res: &httpTestCaseResponse{
				Code: 204,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.DeleteScheduleRequest{
				Id:        "foo",
				RequestId: "DeleteSchedule",
			},
		},
	},

	// Locks
	{
		Name: "AcquireLock",
		Req: &t_api.Request{
			Kind: t_api.AcquireLock,
			Tags: map[string]string{"id": "AcquireLock", "name": "AcquireLock"},
			AcquireLock: &t_api.AcquireLockRequest{
				ResourceId:  "foo",
				ProcessId:   "bar",
				ExecutionId: "baz",
				Ttl:         1,
			},
		},
		Res: &t_api.Response{
			Kind: t_api.AcquireLock,
			AcquireLock: &t_api.AcquireLockResponse{
				Status: t_api.StatusCreated,
				Lock: &lock.Lock{
					ResourceId:  "foo",
					ProcessId:   "bar",
					ExecutionId: "baz",
					Ttl:         1,
				},
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "POST",
				Path:   "locks/acquire",
				Headers: map[string]string{
					"Request-Id": "AcquireLock",
				},
				Body: []byte(`{
					"resourceId": "foo",
					"processId": "bar",
					"executionId": "baz",
					"ttl": 1
				}`),
			},
			Res: &httpTestCaseResponse{
				Code: 201,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.AcquireLockRequest{
				RequestId:   "AcquireLock",
				ResourceId:  "foo",
				ProcessId:   "bar",
				ExecutionId: "baz",
				Ttl:         1,
			},
		},
	},
	{
		Name: "ReleaseLock",
		Req: &t_api.Request{
			Kind: t_api.ReleaseLock,
			Tags: map[string]string{"id": "ReleaseLock", "name": "ReleaseLock"},
			ReleaseLock: &t_api.ReleaseLockRequest{
				ResourceId:  "foo",
				ExecutionId: "bar",
			},
		},
		Res: &t_api.Response{
			Kind: t_api.ReleaseLock,
			ReleaseLock: &t_api.ReleaseLockResponse{
				Status: t_api.StatusNoContent,
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "POST",
				Path:   "locks/release",
				Headers: map[string]string{
					"Request-Id": "ReleaseLock",
				},
				Body: []byte(`{
					"resourceId": "foo",
					"executionId": "bar"
				}`),
			},
			Res: &httpTestCaseResponse{
				Code: 204,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.ReleaseLockRequest{
				RequestId:   "ReleaseLock",
				ResourceId:  "foo",
				ExecutionId: "bar",
			},
		},
	},
	{
		Name: "HeartbeatLocks",
		Req: &t_api.Request{
			Kind: t_api.HeartbeatLocks,
			Tags: map[string]string{"id": "HeartbeatLocks", "name": "HeartbeatLocks"},
			HeartbeatLocks: &t_api.HeartbeatLocksRequest{
				ProcessId: "foo",
			},
		},
		Res: &t_api.Response{
			Kind: t_api.HeartbeatLocks,
			HeartbeatLocks: &t_api.HeartbeatLocksResponse{
				Status:        t_api.StatusOK,
				LocksAffected: 1,
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "POST",
				Path:   "locks/heartbeat",
				Headers: map[string]string{
					"Request-Id": "HeartbeatLocks",
				},
				Body: []byte(`{
					"processId": "foo"
				}`),
			},
			Res: &httpTestCaseResponse{
				Code: 200,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.HeartbeatLocksRequest{
				RequestId: "HeartbeatLocks",
				ProcessId: "foo",
			},
		},
	},

	// Tasks
	{
		Name: "ClaimTask",
		Req: &t_api.Request{
			Kind: t_api.ClaimTask,
			Tags: map[string]string{"id": "ClaimTask", "name": "ClaimTask"},
			ClaimTask: &t_api.ClaimTaskRequest{
				Id:        "foo",
				Counter:   1,
				ProcessId: "bar",
				Ttl:       1,
			},
		},
		Res: &t_api.Response{
			Kind: t_api.ClaimTask,
			ClaimTask: &t_api.ClaimTaskResponse{
				Status: t_api.StatusCreated,
				Task:   &task.Task{Mesg: &message.Mesg{}},
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "POST",
				Path:   "tasks/claim",
				Headers: map[string]string{
					"Request-Id": "ClaimTask",
				},
				Body: []byte(`{
					"id": "foo",
					"counter": 1,
					"processId": "bar",
					"ttl": 1
				}`),
			},
			Res: &httpTestCaseResponse{
				Code: 201,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.ClaimTaskRequest{
				Id:        "foo",
				Counter:   1,
				ProcessId: "bar",
				Ttl:       1,
				RequestId: "ClaimTask",
			},
		},
	},
	{
		Name: "ClaimTaskGet",
		Req: &t_api.Request{
			Kind: t_api.ClaimTask,
			Tags: map[string]string{"id": "ClaimTaskGet", "name": "ClaimTask"},
			ClaimTask: &t_api.ClaimTaskRequest{
				Id:        "foo",
				Counter:   1,
				ProcessId: "foo/1", // default process id for get endpoint
				Ttl:       60000,
			},
		},
		Res: &t_api.Response{
			Kind: t_api.ClaimTask,
			ClaimTask: &t_api.ClaimTaskResponse{
				Status: t_api.StatusCreated,
				Task:   &task.Task{Mesg: &message.Mesg{}},
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "GET",
				Path:   "tasks/claim/foo/1",
				Headers: map[string]string{
					"Request-Id": "ClaimTaskGet",
				},
			},
			Res: &httpTestCaseResponse{
				Code: 201,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.ClaimTaskRequest{
				Id:        "foo",
				Counter:   1,
				ProcessId: "foo/1",
				Ttl:       60000,
				RequestId: "ClaimTaskGet",
			},
		},
	},
	{
		Name: "ClaimTaskInvoke",
		Req: &t_api.Request{
			Kind: t_api.ClaimTask,
			Tags: map[string]string{"id": "ClaimTaskInvoke", "name": "ClaimTask"},
			ClaimTask: &t_api.ClaimTaskRequest{
				Id:        "foo",
				Counter:   1,
				ProcessId: "bar",
				Ttl:       1,
			},
		},
		Res: &t_api.Response{
			Kind: t_api.ClaimTask,
			ClaimTask: &t_api.ClaimTaskResponse{
				Status:          t_api.StatusCreated,
				Task:            &task.Task{Mesg: &message.Mesg{Type: message.Invoke, Root: "foo"}},
				RootPromise:     &promise.Promise{Id: "foo", State: promise.Pending},
				RootPromiseHref: "http://localhost:8001/promises/foo",
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "POST",
				Path:   "tasks/claim",
				Headers: map[string]string{
					"Request-Id": "ClaimTaskInvoke",
				},
				Body: []byte(`{
					"id": "foo",
					"counter": 1,
					"processId": "bar",
					"ttl": 1
				}`),
			},
			Res: &httpTestCaseResponse{
				Code: 201,
				Body: []byte(util.RemoveWhitespace(`{
					"promises":{
						"root":{"data":{"id":"foo","state":"PENDING","param":{},"value":{},"timeout":0},"href":"http://localhost:8001/promises/foo","id":"foo"}
					},
					"type":"invoke"
				}`)),
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.ClaimTaskRequest{
				Id:        "foo",
				Counter:   1,
				ProcessId: "bar",
				Ttl:       1,
				RequestId: "ClaimTaskInvoke",
			},
			Res: &pb.ClaimTaskResponse{
				Claimed: true,
				Mesg: &pb.Mesg{
					Type: "invoke",
					Promises: map[string]*pb.MesgPromise{
						"root": {
							Id:   "foo",
							Href: "http://localhost:8001/promises/foo",
							Data: &pb.Promise{Id: "foo", State: pb.State_PENDING, Param: &pb.Value{}, Value: &pb.Value{}},
						},
					},
				},
			},
		},
	},
	{
		Name: "ClaimTaskResume",
		Req: &t_api.Request{
			Kind: t_api.ClaimTask,
			Tags: map[string]string{
				"id":       "ClaimTaskResume",
				"name":     "ClaimTask",
				"protocol": "http",
			},
			ClaimTask: &t_api.ClaimTaskRequest{
				Id:        "foo",
				Counter:   2,
				ProcessId: "bar",
				Ttl:       1,
			},
		},
		Res: &t_api.Response{
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
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "POST",
				Path:   "tasks/claim",
				Headers: map[string]string{
					"Request-Id": "ClaimTaskResume",
				},
				Body: []byte(`{
					"id": "foo",
					"processId": "bar",
					"counter": 2,
					"ttl": 1
				}`),
			},
			Res: &httpTestCaseResponse{
				Code: 201,
				Body: []byte(util.RemoveWhitespace(`{
					"promises":{
						"leaf":{"data":{"id":"bar","state":"RESOLVED","param":{},"value":{},"timeout":0},"href":"http://localhost:8001/promises/bar","id":"bar"},
						"root":{"data":{"id":"foo","state":"PENDING","param":{},"value":{},"timeout":0},"href":"http://localhost:8001/promises/foo","id":"foo"}
					},
					"type":"resume"
				}`)),
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.ClaimTaskRequest{
				Id:        "foo",
				Counter:   2,
				ProcessId: "bar",
				Ttl:       1,
				RequestId: "ClaimTaskResume",
			},
			Res: &pb.ClaimTaskResponse{
				Claimed: true,
				Mesg: &pb.Mesg{
					Type: "resume",
					Promises: map[string]*pb.MesgPromise{
						"root": {
							Id:   "foo",
							Href: "http://localhost:8001/promises/foo",
							Data: &pb.Promise{Id: "foo", State: pb.State_PENDING, Param: &pb.Value{}, Value: &pb.Value{}},
						},
						"leaf": {
							Id:   "bar",
							Href: "http://localhost:8001/promises/bar",
							Data: &pb.Promise{Id: "bar", State: pb.State_RESOLVED, Param: &pb.Value{}, Value: &pb.Value{}},
						},
					},
				},
			},
		},
	},
	{
		Name: "CompleteTask",
		Req: &t_api.Request{
			Kind: t_api.CompleteTask,
			Tags: map[string]string{"id": "CompleteTask", "name": "CompleteTask"},
			CompleteTask: &t_api.CompleteTaskRequest{
				Id:      "foo",
				Counter: 1,
			},
		},
		Res: &t_api.Response{
			Kind: t_api.CompleteTask,
			CompleteTask: &t_api.CompleteTaskResponse{
				Status: t_api.StatusCreated,
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "POST",
				Path:   "tasks/complete",
				Headers: map[string]string{
					"Request-Id": "CompleteTask",
				},
				Body: []byte(`{
					"id": "foo",
					"counter": 1
				}`),
			},
			Res: &httpTestCaseResponse{
				Code: 201,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.CompleteTaskRequest{
				Id:        "foo",
				Counter:   1,
				RequestId: "CompleteTask",
			},
		},
	},
	{
		Name: "CompleteTaskGet",
		Req: &t_api.Request{
			Kind: t_api.CompleteTask,
			Tags: map[string]string{"id": "CompleteTaskGet", "name": "CompleteTask"},
			CompleteTask: &t_api.CompleteTaskRequest{
				Id:      "foo",
				Counter: 1,
			},
		},
		Res: &t_api.Response{
			Kind: t_api.CompleteTask,
			CompleteTask: &t_api.CompleteTaskResponse{
				Status: t_api.StatusCreated,
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "GET",
				Path:   "tasks/complete/foo/1",
				Headers: map[string]string{
					"Request-Id": "CompleteTaskGet",
				},
			},
			Res: &httpTestCaseResponse{
				Code: 201,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.CompleteTaskRequest{
				Id:        "foo",
				Counter:   1,
				RequestId: "CompleteTaskGet",
			},
		},
	},
	{
		Name: "HeartbeatTasks",
		Req: &t_api.Request{
			Kind: t_api.HeartbeatTasks,
			Tags: map[string]string{"id": "HeartbeatTasks", "name": "HeartbeatTasks"},
			HeartbeatTasks: &t_api.HeartbeatTasksRequest{
				ProcessId: "foo",
			},
		},
		Res: &t_api.Response{
			Kind: t_api.HeartbeatTasks,
			HeartbeatTasks: &t_api.HeartbeatTasksResponse{
				Status:        t_api.StatusOK,
				TasksAffected: 1,
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "POST",
				Path:   "tasks/heartbeat",
				Headers: map[string]string{
					"Request-Id": "HeartbeatTasks",
				},
				Body: []byte(`{
					"processId": "foo"
				}`),
			},
			Res: &httpTestCaseResponse{
				Code: 200,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.HeartbeatTasksRequest{
				ProcessId: "foo",
				RequestId: "HeartbeatTasks",
			},
		},
	},
	{
		Name: "HeartbeatTasksGet",
		Req: &t_api.Request{
			Kind: t_api.HeartbeatTasks,
			Tags: map[string]string{"id": "HeartbeatTasksGet", "name": "HeartbeatTasks"},
			HeartbeatTasks: &t_api.HeartbeatTasksRequest{
				ProcessId: "foo/1",
			},
		},
		Res: &t_api.Response{
			Kind: t_api.HeartbeatTasks,
			HeartbeatTasks: &t_api.HeartbeatTasksResponse{
				Status:        t_api.StatusOK,
				TasksAffected: 1,
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "GET",
				Path:   "tasks/heartbeat/foo/1",
				Headers: map[string]string{
					"Request-Id": "HeartbeatTasksGet",
				},
			},
			Res: &httpTestCaseResponse{
				Code: 200,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.HeartbeatTasksRequest{
				ProcessId: "foo/1",
				RequestId: "HeartbeatTasksGet",
			},
		},
	},
	{
		Name: "CreatePromiseAndTaskWithTtlZero",
		Req: &t_api.Request{
			Kind: t_api.CreatePromiseAndTask,
			Tags: map[string]string{"id": "CreatePromiseAndTask", "name": "CreatePromiseAndTask"},
			CreatePromiseAndTask: &t_api.CreatePromiseAndTaskRequest{
				Promise: &t_api.CreatePromiseRequest{
					Id:      "foo",
					Timeout: 1,
					Tags:    map[string]string{"resonate:invoke": "baz"},
				},
				Task: &t_api.CreateTaskRequest{
					PromiseId: "foo",
					ProcessId: "bar",
					Ttl:       0,
					Timeout:   1,
				},
			},
		},
		Res: &t_api.Response{
			Kind: t_api.CreatePromiseAndTask,
			CreatePromiseAndTask: &t_api.CreatePromiseAndTaskResponse{
				Status: t_api.StatusCreated,
				Promise: &promise.Promise{
					Id:      "foo",
					State:   promise.Pending,
					Param:   promise.Value{},
					Timeout: 1,
				},
				Task: &task.Task{},
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "POST",
				Path:   "promises/task",
				Headers: map[string]string{
					"Request-Id": "CreatePromiseAndTask",
				},
				Body: []byte(`{
					"promise": {"id": "foo", "timeout": 1, "tags": {"resonate:invoke": "baz"}},
					"task": {"processId": "bar", "ttl": 0}
				}`),
			},
			Res: &httpTestCaseResponse{
				Code: 201,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.CreatePromiseAndTaskRequest{
				Promise: &pb.CreatePromiseRequest{
					Id:        "foo",
					Timeout:   1,
					RequestId: "CreatePromiseAndTask",
					Tags:      map[string]string{"resonate:invoke": "baz"},
				},
				Task: &pb.CreatePromiseTaskRequest{
					ProcessId: "bar",
					Ttl:       0,
				},
			},
			Res: &pb.CreatePromiseAndTaskResponse{
				Noop: false,
				Promise: &pb.Promise{
					Id:                      "foo",
					State:                   pb.State_PENDING,
					IdempotencyKeyForCreate: "",
					Param:                   &pb.Value{},
					Value:                   &pb.Value{},
					Timeout:                 1,
				},
			},
		},
	},
	{
		Name: "CreatePromiseAndTaskWithNegativeTtl",
		Req: &t_api.Request{
			Kind: t_api.CreatePromiseAndTask,
			Tags: map[string]string{"id": "CreatePromiseAndTask", "name": "CreatePromiseAndTask"},
			CreatePromiseAndTask: &t_api.CreatePromiseAndTaskRequest{
				Promise: &t_api.CreatePromiseRequest{
					Id:      "foo",
					Timeout: 1,
					Tags:    map[string]string{"resonate:invoke": "baz"},
				},
				Task: &t_api.CreateTaskRequest{
					PromiseId: "foo",
					ProcessId: "bar",
					Ttl:       -1,
					Timeout:   1,
				},
			},
		},
		Res: &t_api.Response{
			Kind: t_api.CreatePromiseAndTask,
			CreatePromiseAndTask: &t_api.CreatePromiseAndTaskResponse{
				Status: t_api.StatusCreated,
				Promise: &promise.Promise{
					Id:      "foo",
					State:   promise.Pending,
					Param:   promise.Value{},
					Timeout: 1,
				},
				Task: &task.Task{},
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "POST",
				Path:   "promises/task",
				Headers: map[string]string{
					"Request-Id": "CreatePromiseAndTask",
				},
				Body: []byte(`{
					"promise": {"id": "foo", "timeout": 1, "tags": {"resonate:invoke": "baz"}},
					"task": {"processId": "bar", "ttl": -1}
				}`),
			},
			Res: &httpTestCaseResponse{
				Code: 400,
			},
		},
		Grpc: &grpcTestCase{
			Req: &pb.CreatePromiseAndTaskRequest{
				Promise: &pb.CreatePromiseRequest{
					Id:        "foo",
					Timeout:   1,
					RequestId: "CreatePromiseAndTask",
					Tags:      map[string]string{"resonate:invoke": "baz"},
				},
				Task: &pb.CreatePromiseTaskRequest{
					ProcessId: "bar",
					Ttl:       -1,
				},
			},
			Code: codes.InvalidArgument,
		},
	},
}

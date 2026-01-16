package test

import (
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/message"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/schedule"
	"github.com/resonatehq/resonate/pkg/task"
)

type testCase struct {
	Name   string
	Req    *t_api.Request
	Res    *t_api.Response
	Http   *httpTestCase
	NoAuth bool
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

var TestCases = []*testCase{
	// Ping
	{
		Name:   "Ping",
		NoAuth: true,
		Req:    nil,
		Res:    nil,
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "GET",
				Path:   "ping",
			},
			Res: &httpTestCaseResponse{
				Code: 200,
				Body: []byte(`{"status":"ok"}`),
			},
		},
	},
	// Promises
	{
		Name: "ReadPromise",
		Req: &t_api.Request{
			Head: map[string]string{"id": "ReadPromise", "name": "promise.get"},
			Data: &t_api.PromiseGetRequest{
				Id: "foo",
			},
		},
		Res: &t_api.Response{
			Status: t_api.StatusOK,
			Payload: &t_api.PromiseGetResponse{
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
	},
	{
		Name: "ReadPromiseWithSlash",
		Req: &t_api.Request{
			Head: map[string]string{"id": "ReadPromiseWithSlash", "name": "promise.get"},
			Data: &t_api.PromiseGetRequest{
				Id: "foo/bar",
			},
		},
		Res: &t_api.Response{
			Status: t_api.StatusOK,
			Payload: &t_api.PromiseGetResponse{
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
	},
	{
		Name: "ReadPromiseNotFound",
		Req: &t_api.Request{
			Head: map[string]string{"id": "ReadPromiseNotFound", "name": "promise.get"},
			Data: &t_api.PromiseGetRequest{
				Id: "bar",
			},
		},
		Res: &t_api.Response{
			Status: t_api.StatusPromiseNotFound,
			Payload: &t_api.PromiseGetResponse{
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
	},
	{
		Name: "SearchPromises",
		Req: &t_api.Request{
			Head: map[string]string{"id": "SearchPromises", "name": "promise.search"},
			Data: &t_api.PromiseSearchRequest{
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
			Status: t_api.StatusOK,
			Payload: &t_api.PromiseSearchResponse{
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
	},
	{
		Name: "SearchPromisesCursor",
		Req: &t_api.Request{
			Head: map[string]string{"id": "SearchPromisesCursor", "name": "promise.search"},
			Data: &t_api.PromiseSearchRequest{
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
			Status: t_api.StatusOK,
			Payload: &t_api.PromiseSearchResponse{
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
	},
	{
		Name: "SearchPromisesPending",
		Req: &t_api.Request{
			Head: map[string]string{"id": "SearchPromisesPending", "name": "promise.search"},
			Data: &t_api.PromiseSearchRequest{
				Id: "*",
				States: []promise.State{
					promise.Pending,
				},
				Tags:  map[string]string{},
				Limit: 10,
			},
		},
		Res: &t_api.Response{
			Status: t_api.StatusOK,
			Payload: &t_api.PromiseSearchResponse{
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
	},
	{
		Name: "SearchPromisesResolved",
		Req: &t_api.Request{
			Head: map[string]string{"id": "SearchPromisesResolved", "name": "promise.search"},
			Data: &t_api.PromiseSearchRequest{
				Id: "*",
				States: []promise.State{
					promise.Resolved,
				},
				Tags:  map[string]string{},
				Limit: 10,
			},
		},
		Res: &t_api.Response{
			Status: t_api.StatusOK,
			Payload: &t_api.PromiseSearchResponse{
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
	},
	{
		Name: "SearchPromisesRejected",
		Req: &t_api.Request{
			Head: map[string]string{"id": "SearchPromisesRejected", "name": "promise.search"},
			Data: &t_api.PromiseSearchRequest{
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
			Status: t_api.StatusOK,
			Payload: &t_api.PromiseSearchResponse{
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
	},
	{
		Name: "SearchPromisesTags",
		Req: &t_api.Request{
			Head: map[string]string{"id": "SearchPromisesTags", "name": "promise.search"},
			Data: &t_api.PromiseSearchRequest{
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
			Status: t_api.StatusOK,
			Payload: &t_api.PromiseSearchResponse{
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
	},
	{
		Name: "SearchPromisesDefaultLimit",
		Req: &t_api.Request{
			Head: map[string]string{"id": "SearchPromisesDefaultLimit", "name": "promise.search"},
			Data: &t_api.PromiseSearchRequest{
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
			Status: t_api.StatusOK,
			Payload: &t_api.PromiseSearchResponse{
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
	},
	{
		Name: "CreatePromise",
		Req: &t_api.Request{
			Head: map[string]string{"id": "CreatePromise", "name": "promise.create"},
			Data: &t_api.PromiseCreateRequest{
				Id: "foo",
				Param: promise.Value{
					Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
					Data:    []byte("pending"),
				},
				Timeout: 1,
			},
		},
		Res: &t_api.Response{
			Status: t_api.StatusCreated,
			Payload: &t_api.PromiseCreateResponse{
				Promise: &promise.Promise{
					Id:    "foo",
					State: promise.Pending,
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
					"Request-Id": "CreatePromise",
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
	},
	{
		Name: "CreatePromiseMinimal",
		Req: &t_api.Request{
			Head: map[string]string{"id": "CreatePromiseMinimal", "name": "promise.create"},
			Data: &t_api.PromiseCreateRequest{
				Id:      "foo",
				Param:   promise.Value{},
				Timeout: 1,
			},
		},
		Res: &t_api.Response{
			Status: t_api.StatusCreated,
			Payload: &t_api.PromiseCreateResponse{
				Promise: &promise.Promise{
					Id:      "foo",
					State:   promise.Pending,
					Param:   promise.Value{},
					Timeout: 1,
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
	},
	{
		Name: "CreatePromiseWithTraceContext",
		Req: &t_api.Request{
			Head: map[string]string{"id": "CreatePromiseWithTraceContext", "name": "promise.create", "traceparent": "foo", "tracestate": "bar"},
			Data: &t_api.PromiseCreateRequest{
				Id:      "foo",
				Param:   promise.Value{},
				Timeout: 1,
			},
		},
		Res: &t_api.Response{
			Status: t_api.StatusCreated,
			Payload: &t_api.PromiseCreateResponse{
				Promise: &promise.Promise{
					Id:      "foo",
					State:   promise.Pending,
					Param:   promise.Value{},
					Timeout: 1,
				},
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "POST",
				Path:   "promises",
				Headers: map[string]string{
					"Request-Id":  "CreatePromiseWithTraceContext",
					"Traceparent": "foo",
					"Tracestate":  "bar",
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
	},
	{
		Name: "CreatePromiseAndTask",
		Req: &t_api.Request{
			Head: map[string]string{"id": "CreatePromiseAndTask", "name": "task.create"},
			Data: &t_api.TaskCreateRequest{
				Promise: &t_api.PromiseCreateRequest{
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
			Status: t_api.StatusCreated,
			Payload: &t_api.TaskCreateResponse{
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
	},
	{
		Name: "CreatePromiseAndTaskWithTraceContext",
		Req: &t_api.Request{
			Head: map[string]string{"id": "CreatePromiseAndTaskWithTraceContext", "name": "task.create", "traceparent": "baz", "tracestate": "qux"},
			Data: &t_api.TaskCreateRequest{
				Promise: &t_api.PromiseCreateRequest{
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
			Status: t_api.StatusCreated,
			Payload: &t_api.TaskCreateResponse{
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
					"Request-Id":  "CreatePromiseAndTaskWithTraceContext",
					"Traceparent": "baz",
					"Tracestate":  "qux",
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
	},
	{
		Name: "CreatePromiseTimeoutTooLarge",
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "POST",
				Path:   "promises",
				Headers: map[string]string{
					"Request-Id": "CreatePromiseTimeoutTooLarge",
				},
				Body: []byte(`{
					"id": "foo",
					"timeout": 9223372036854775808
				}`),
			},
			Res: &httpTestCaseResponse{
				Code: 400,
			},
		},
	},
	{
		Name: "ResolvePromise",
		Req: &t_api.Request{
			Head: map[string]string{"id": "ResolvePromise", "name": "promise.complete"},
			Data: &t_api.PromiseCompleteRequest{
				Id:    "foo",
				State: promise.Resolved,
				Value: promise.Value{
					Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
					Data:    []byte("resolve"),
				},
			},
		},
		Res: &t_api.Response{
			Status: t_api.StatusCreated,
			Payload: &t_api.PromiseCompleteResponse{
				Promise: &promise.Promise{
					Id:    "foo",
					State: promise.Resolved,
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
					"Request-Id": "ResolvePromise",
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
	},
	{
		Name: "ResolvePromiseMinimal",
		Req: &t_api.Request{
			Head: map[string]string{"id": "ResolvePromiseMinimal", "name": "promise.complete"},
			Data: &t_api.PromiseCompleteRequest{
				Id:    "foo",
				State: promise.Resolved,
				Value: promise.Value{},
			},
		},
		Res: &t_api.Response{
			Status: t_api.StatusCreated,
			Payload: &t_api.PromiseCompleteResponse{
				Promise: &promise.Promise{
					Id:    "foo",
					State: promise.Resolved,
					Value: promise.Value{},
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
	},
	{
		Name: "RejectPromise",
		Req: &t_api.Request{
			Head: map[string]string{"id": "RejectPromise", "name": "promise.complete"},
			Data: &t_api.PromiseCompleteRequest{
				Id:    "foo",
				State: promise.Rejected,
				Value: promise.Value{
					Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
					Data:    []byte("reject"),
				},
			},
		},
		Res: &t_api.Response{
			Status: t_api.StatusCreated,
			Payload: &t_api.PromiseCompleteResponse{
				Promise: &promise.Promise{
					Id:    "foo",
					State: promise.Rejected,
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
					"Request-Id": "RejectPromise",
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
	},
	{
		Name: "RejectPromiseMinimal",
		Req: &t_api.Request{
			Head: map[string]string{"id": "RejectPromiseMinimal", "name": "promise.complete"},
			Data: &t_api.PromiseCompleteRequest{
				Id:    "foo",
				State: promise.Rejected,
				Value: promise.Value{},
			},
		},
		Res: &t_api.Response{
			Status: t_api.StatusCreated,
			Payload: &t_api.PromiseCompleteResponse{
				Promise: &promise.Promise{
					Id:    "foo",
					State: promise.Rejected,
					Value: promise.Value{},
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
	},
	{
		Name: "CancelPromise",
		Req: &t_api.Request{
			Head: map[string]string{"id": "CancelPromise", "name": "promise.complete"},
			Data: &t_api.PromiseCompleteRequest{
				Id:    "foo",
				State: promise.Canceled,
				Value: promise.Value{
					Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
					Data:    []byte("cancel"),
				},
			},
		},
		Res: &t_api.Response{
			Status: t_api.StatusCreated,
			Payload: &t_api.PromiseCompleteResponse{
				Promise: &promise.Promise{
					Id:    "foo",
					State: promise.Canceled,
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
					"Request-Id": "CancelPromise",
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
	},
	{
		Name: "CancelPromiseMinimal",
		Req: &t_api.Request{
			Head: map[string]string{"id": "CancelPromiseMinimal", "name": "promise.complete"},
			Data: &t_api.PromiseCompleteRequest{
				Id:    "foo",
				State: promise.Canceled,
				Value: promise.Value{},
			},
		},
		Res: &t_api.Response{
			Status: t_api.StatusCreated,
			Payload: &t_api.PromiseCompleteResponse{
				Promise: &promise.Promise{
					Id:    "foo",
					State: promise.Canceled,
					Value: promise.Value{},
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
	},

	// Callbacks
	{
		Name: "CreateCallbackLogicalReceiver",
		Req: &t_api.Request{
			Head: map[string]string{"id": "CreateCallback", "name": "promise.register"},
			Data: &t_api.PromiseRegisterRequest{
				Id:        "__resume:bar:foo",
				PromiseId: "foo",
				Recv:      []byte(`"foo"`),
				Mesg:      &message.Mesg{Type: "resume", Head: map[string]string{}, Root: "bar", Leaf: "foo"},
				Timeout:   1,
			},
		},
		Res: &t_api.Response{
			Status:  t_api.StatusCreated,
			Payload: &t_api.PromiseRegisterResponse{},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "POST",
				Path:   "promises/callback/foo",
				Headers: map[string]string{
					"Request-Id": "CreateCallback",
				},
				Body: []byte(`{
					"rootPromiseId": "bar",
					"timeout": 1,
					"recv": "foo"
				}`),
			},
			Res: &httpTestCaseResponse{
				Code: 201,
			},
		},
	},
	{
		Name: "CreateCallbackLogicalReceiverDeprecated",
		Req: &t_api.Request{
			Head: map[string]string{"id": "CreateCallback", "name": "promise.register"},
			Data: &t_api.PromiseRegisterRequest{
				Id:        "__resume:bar:foo",
				PromiseId: "foo",
				Recv:      []byte(`"foo"`),
				Mesg:      &message.Mesg{Type: "resume", Head: map[string]string{}, Root: "bar", Leaf: "foo"},
				Timeout:   1,
			},
		},
		Res: &t_api.Response{
			Status:  t_api.StatusCreated,
			Payload: &t_api.PromiseRegisterResponse{},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "POST",
				Path:   "callbacks",
				Headers: map[string]string{
					"Request-Id": "CreateCallback",
				},
				Body: []byte(`{
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
	},
	{
		Name: "CreateCallbackPhysicalReceiver",
		Req: &t_api.Request{
			Head: map[string]string{"id": "CreateCallbackPhysicalReceiver", "name": "promise.register"},
			Data: &t_api.PromiseRegisterRequest{
				Id:        "__resume:bar:foo",
				PromiseId: "foo",
				Recv:      []byte(`{"type":"http","data":{"url":"http://localhost:3000"}}`),
				Mesg:      &message.Mesg{Type: "resume", Head: map[string]string{}, Root: "bar", Leaf: "foo"},
				Timeout:   1,
			},
		},
		Res: &t_api.Response{
			Status:  t_api.StatusCreated,
			Payload: &t_api.PromiseRegisterResponse{},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "POST",
				Path:   "promises/callback/foo",
				Headers: map[string]string{
					"Request-Id": "CreateCallbackPhysicalReceiver",
				},
				Body: []byte(`{
					"rootPromiseId": "bar",
					"timeout": 1,
					"recv": {"type":"http","data":{"url":"http://localhost:3000"}}
				}`),
			},
			Res: &httpTestCaseResponse{
				Code: 201,
			},
		},
	},
	{
		Name: "CreateCallbackPhysicalReceiverDeprecated",
		Req: &t_api.Request{
			Head: map[string]string{"id": "CreateCallbackPhysicalReceiver", "name": "promise.register"},
			Data: &t_api.PromiseRegisterRequest{
				Id:        "__resume:bar:foo",
				PromiseId: "foo",
				Recv:      []byte(`{"type":"http","data":{"url":"http://localhost:3000"}}`),
				Mesg:      &message.Mesg{Type: "resume", Head: map[string]string{}, Root: "bar", Leaf: "foo"},
				Timeout:   1,
			},
		},
		Res: &t_api.Response{
			Status:  t_api.StatusCreated,
			Payload: &t_api.PromiseRegisterResponse{},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "POST",
				Path:   "callbacks",
				Headers: map[string]string{
					"Request-Id": "CreateCallbackPhysicalReceiver",
				},
				Body: []byte(`{
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
	},
	{
		Name: "CreateCallbackWithTraceContext",
		Req: &t_api.Request{
			Head: map[string]string{"id": "CreateCallbackWithTraceContext", "name": "promise.register"},
			Data: &t_api.PromiseRegisterRequest{
				Id:        "__resume:bar:foo",
				PromiseId: "foo",
				Recv:      []byte(`{"type":"http","data":{"url":"http://localhost:3000"}}`),
				Mesg:      &message.Mesg{Type: "resume", Head: map[string]string{"traceparent": "foo", "tracestate": "bar"}, Root: "bar", Leaf: "foo"},
				Timeout:   1,
			},
		},
		Res: &t_api.Response{
			Status:  t_api.StatusCreated,
			Payload: &t_api.PromiseRegisterResponse{},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "POST",
				Path:   "promises/callback/foo",
				Headers: map[string]string{
					"Request-Id":  "CreateCallbackWithTraceContext",
					"Traceparent": "foo",
					"Tracestate":  "bar",
				},
				Body: []byte(`{
					"rootPromiseId": "bar",
					"timeout": 1,
					"recv": {"type":"http","data":{"url":"http://localhost:3000"}}
				}`),
			},
			Res: &httpTestCaseResponse{
				Code: 201,
			},
		},
	},
	{
		Name: "CreateCallbackNotFound",
		Req: &t_api.Request{
			Head: map[string]string{"id": "CreateCallbackNotFound", "name": "promise.register"},
			Data: &t_api.PromiseRegisterRequest{
				Id:        "__resume:bar:foo",
				PromiseId: "foo",
				Recv:      []byte(`"foo"`),
				Mesg:      &message.Mesg{Type: "resume", Head: map[string]string{}, Root: "bar", Leaf: "foo"},
				Timeout:   1,
			},
		},
		Res: &t_api.Response{
			Status:  t_api.StatusPromiseNotFound,
			Payload: &t_api.PromiseRegisterResponse{},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "POST",
				Path:   "promises/callback/foo",
				Headers: map[string]string{
					"Request-Id": "CreateCallbackNotFound",
				},
				Body: []byte(`{
					"rootPromiseId": "bar",
					"timeout": 1,
					"recv": "foo"
				}`),
			},
			Res: &httpTestCaseResponse{
				Code: 404,
			},
		},
	},
	{
		Name: "CreateCallbackNotFoundDeprecated",
		Req: &t_api.Request{
			Head: map[string]string{"id": "CreateCallbackNotFound", "name": "promise.register"},
			Data: &t_api.PromiseRegisterRequest{
				Id:        "__resume:bar:foo",
				PromiseId: "foo",
				Recv:      []byte(`"foo"`),
				Mesg:      &message.Mesg{Type: "resume", Head: map[string]string{}, Root: "bar", Leaf: "foo"},
				Timeout:   1,
			},
		},
		Res: &t_api.Response{
			Status:  t_api.StatusPromiseNotFound,
			Payload: &t_api.PromiseRegisterResponse{},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "POST",
				Path:   "callbacks",
				Headers: map[string]string{
					"Request-Id": "CreateCallbackNotFound",
				},
				Body: []byte(`{
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
	},

	// Subscriptions
	{
		Name: "CreateSubscriptionLogicalReceiver",
		Req: &t_api.Request{
			Head: map[string]string{"id": "CreateSubscription", "name": "promise.register"},
			Data: &t_api.PromiseRegisterRequest{
				Id:        "__notify:foo:foo.1",
				PromiseId: "foo",
				Recv:      []byte(`"foo"`),
				Mesg:      &message.Mesg{Type: "notify", Head: map[string]string{}, Root: "foo"},
				Timeout:   1,
			},
		},
		Res: &t_api.Response{
			Status:  t_api.StatusCreated,
			Payload: &t_api.PromiseRegisterResponse{},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "POST",
				Path:   "promises/subscribe/foo",
				Headers: map[string]string{
					"Request-Id": "CreateSubscription",
				},
				Body: []byte(`{
					"id": "foo.1",
					"timeout": 1,
					"recv": "foo"
				}`),
			},
			Res: &httpTestCaseResponse{
				Code: 201,
			},
		},
	},
	{
		Name: "CreateSubscriptionLogicalReceiverDeprecated",
		Req: &t_api.Request{
			Head: map[string]string{"id": "CreateSubscription", "name": "promise.register"},
			Data: &t_api.PromiseRegisterRequest{
				Id:        "__notify:foo:foo.1",
				PromiseId: "foo",
				Recv:      []byte(`"foo"`),
				Mesg:      &message.Mesg{Type: "notify", Head: map[string]string{}, Root: "foo"},
				Timeout:   1,
			},
		},
		Res: &t_api.Response{
			Status:  t_api.StatusCreated,
			Payload: &t_api.PromiseRegisterResponse{},
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
	},
	{
		Name: "CreateSubscriptionPhysicalReceiver",
		Req: &t_api.Request{
			Head: map[string]string{"id": "CreateSubscriptionPhysicalRecv", "name": "promise.register"},
			Data: &t_api.PromiseRegisterRequest{
				Id:        "__notify:foo:foo.1",
				PromiseId: "foo",
				Recv:      []byte(`{"type":"http","data":{"url":"http://localhost:3000"}}`),
				Mesg:      &message.Mesg{Type: "notify", Head: map[string]string{}, Root: "foo"},
				Timeout:   1,
			},
		},
		Res: &t_api.Response{
			Status:  t_api.StatusCreated,
			Payload: &t_api.PromiseRegisterResponse{},
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
	},
	{
		Name: "CreateSubscriptionPhysicalReceiverDeprecated",
		Req: &t_api.Request{
			Head: map[string]string{"id": "CreateSubscriptionPhysicalRecv", "name": "promise.register"},
			Data: &t_api.PromiseRegisterRequest{
				Id:        "__notify:foo:foo.1",
				PromiseId: "foo",
				Recv:      []byte(`{"type":"http","data":{"url":"http://localhost:3000"}}`),
				Mesg:      &message.Mesg{Type: "notify", Head: map[string]string{}, Root: "foo"},
				Timeout:   1,
			},
		},
		Res: &t_api.Response{
			Status:  t_api.StatusCreated,
			Payload: &t_api.PromiseRegisterResponse{},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "POST",
				Path:   "promises/subscribe/foo",
				Headers: map[string]string{
					"Request-Id": "CreateSubscriptionPhysicalRecv",
				},
				Body: []byte(`{
					"id": "foo.1",
					"timeout": 1,
					"recv": {"type":"http","data":{"url":"http://localhost:3000"}}
				}`),
			},
			Res: &httpTestCaseResponse{
				Code: 201,
			},
		},
	},
	{
		Name: "CreateSubscriptionWithTraceContext",
		Req: &t_api.Request{
			Head: map[string]string{"id": "CreateSubscriptionWithTraceContext", "name": "promise.register"},
			Data: &t_api.PromiseRegisterRequest{
				Id:        "__notify:foo:foo.1",
				PromiseId: "foo",
				Recv:      []byte(`{"type":"http","data":{"url":"http://localhost:3000"}}`),
				Mesg:      &message.Mesg{Type: "notify", Head: map[string]string{"traceparent": "baz", "tracestate": "qux"}, Root: "foo"},
				Timeout:   1,
			},
		},
		Res: &t_api.Response{
			Status:  t_api.StatusCreated,
			Payload: &t_api.PromiseRegisterResponse{},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "POST",
				Path:   "subscriptions",
				Headers: map[string]string{
					"Request-Id":  "CreateSubscriptionWithTraceContext",
					"Traceparent": "baz",
					"Tracestate":  "qux",
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
	},
	{
		Name: "CreateSubscriptionNotFound",
		Req: &t_api.Request{
			Head: map[string]string{"id": "CreateSubscriptionNotFound", "name": "promise.register"},
			Data: &t_api.PromiseRegisterRequest{
				Id:        "__notify:foo:foo.1",
				PromiseId: "foo",
				Recv:      []byte(`"foo"`),
				Mesg:      &message.Mesg{Type: "notify", Head: map[string]string{}, Root: "foo"},
				Timeout:   1,
			},
		},
		Res: &t_api.Response{
			Status:  t_api.StatusPromiseNotFound,
			Payload: &t_api.PromiseRegisterResponse{},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "POST",
				Path:   "promises/subscribe/foo",
				Headers: map[string]string{
					"Request-Id": "CreateSubscriptionNotFound",
				},
				Body: []byte(`{
					"id": "foo.1",
					"timeout": 1,
					"recv": "foo"
				}`),
			},
			Res: &httpTestCaseResponse{
				Code: 404,
			},
		},
	},
	{
		Name: "CreateSubscriptionNotFoundDeprecated",
		Req: &t_api.Request{
			Head: map[string]string{"id": "CreateSubscriptionNotFound", "name": "promise.register"},
			Data: &t_api.PromiseRegisterRequest{
				Id:        "__notify:foo:foo.1",
				PromiseId: "foo",
				Recv:      []byte(`"foo"`),
				Mesg:      &message.Mesg{Type: "notify", Head: map[string]string{}, Root: "foo"},
				Timeout:   1,
			},
		},
		Res: &t_api.Response{
			Status:  t_api.StatusPromiseNotFound,
			Payload: &t_api.PromiseRegisterResponse{},
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
	},
	// Schedules
	{
		Name: "ReadSchedule",
		Req: &t_api.Request{
			Head: map[string]string{"id": "ReadSchedule", "name": "schedule.get"},
			Data: &t_api.ScheduleGetRequest{
				Id: "foo",
			},
		},
		Res: &t_api.Response{
			Status: t_api.StatusOK,
			Payload: &t_api.ScheduleGetResponse{
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
	},
	{
		Name: "SearchSchedules",
		Req: &t_api.Request{
			Head: map[string]string{"id": "SearchSchedules", "name": "schedule.search"},
			Data: &t_api.ScheduleSearchRequest{
				Id:    "*",
				Tags:  map[string]string{},
				Limit: 10,
			},
		},
		Res: &t_api.Response{
			Status: t_api.StatusOK,
			Payload: &t_api.ScheduleSearchResponse{
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
	},
	{
		Name: "SearchSchedulesCursor",
		Req: &t_api.Request{
			Head: map[string]string{"id": "SearchSchedulesCursor", "name": "schedule.search"},
			Data: &t_api.ScheduleSearchRequest{
				Id:     "*",
				Tags:   map[string]string{},
				Limit:  10,
				SortId: util.ToPointer(int64(100)),
			},
		},
		Res: &t_api.Response{
			Status: t_api.StatusOK,
			Payload: &t_api.ScheduleSearchResponse{
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
	},
	{
		Name: "SearchSchedulesTags",
		Req: &t_api.Request{
			Head: map[string]string{"id": "SearchSchedulesTags", "name": "schedule.search"},
			Data: &t_api.ScheduleSearchRequest{
				Id: "*",
				Tags: map[string]string{
					"foo": "bar",
				},
				Limit: 10,
			},
		},
		Res: &t_api.Response{
			Status: t_api.StatusOK,
			Payload: &t_api.ScheduleSearchResponse{
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
	},
	{
		Name: "SearchSchedulesDefaultLimit",
		Req: &t_api.Request{
			Head: map[string]string{"id": "SearchSchedulesDefaultLimit", "name": "schedule.search"},
			Data: &t_api.ScheduleSearchRequest{
				Id:    "*",
				Tags:  map[string]string{},
				Limit: 100,
			},
		},
		Res: &t_api.Response{
			Status: t_api.StatusOK,
			Payload: &t_api.ScheduleSearchResponse{
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
	},
	{
		Name: "CreateSchedule",
		Req: &t_api.Request{
			Head: map[string]string{"id": "CreateSchedule", "name": "schedule.create"},
			Data: &t_api.ScheduleCreateRequest{
				Id:             "foo",
				Cron:           "* * * * *",
				PromiseId:      "foo.{{.timestamp}}",
				PromiseTimeout: 1,
			},
		},
		Res: &t_api.Response{
			Status: t_api.StatusCreated,
			Payload: &t_api.ScheduleCreateResponse{
				Schedule: &schedule.Schedule{
					Id:             "foo",
					Description:    "",
					Cron:           "* * * * *",
					PromiseId:      "foo.{{.timestamp}}",
					PromiseTimeout: 1,
				},
			},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "POST",
				Path:   "schedules",
				Headers: map[string]string{
					"Request-Id": "CreateSchedule",
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
	},
	{
		Name: "DeleteSchedule",
		Req: &t_api.Request{
			Head: map[string]string{"id": "DeleteSchedule", "name": "schedule.delete"},
			Data: &t_api.ScheduleDeleteRequest{
				Id: "foo",
			},
		},
		Res: &t_api.Response{
			Status:  t_api.StatusNoContent,
			Payload: &t_api.ScheduleDeleteResponse{},
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
	},

	// Tasks
	{
		Name: "ClaimTask",
		Req: &t_api.Request{
			Head: map[string]string{"id": "ClaimTask", "name": "task.acquire"},
			Data: &t_api.TaskAcquireRequest{
				Id:        "foo",
				Counter:   1,
				ProcessId: "bar",
				Ttl:       1,
			},
		},
		Res: &t_api.Response{
			Status: t_api.StatusCreated,
			Payload: &t_api.TaskAcquireResponse{
				Task: &task.Task{Mesg: &message.Mesg{}},
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
	},
	{
		Name: "ClaimTaskGet",
		Req: &t_api.Request{
			Head: map[string]string{"id": "ClaimTaskGet", "name": "task.acquire"},
			Data: &t_api.TaskAcquireRequest{
				Id:        "foo",
				Counter:   1,
				ProcessId: "foo/1", // default process id for get endpoint
				Ttl:       60000,
			},
		},
		Res: &t_api.Response{
			Status: t_api.StatusCreated,
			Payload: &t_api.TaskAcquireResponse{
				Task: &task.Task{Mesg: &message.Mesg{}},
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
	},
	{
		Name: "ClaimTaskInvoke",
		Req: &t_api.Request{
			Head: map[string]string{"id": "ClaimTaskInvoke", "name": "task.acquire"},
			Data: &t_api.TaskAcquireRequest{
				Id:        "foo",
				Counter:   1,
				ProcessId: "bar",
				Ttl:       1,
			},
		},
		Res: &t_api.Response{
			Status: t_api.StatusCreated,
			Payload: &t_api.TaskAcquireResponse{
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
	},
	{
		Name: "ClaimTaskResume",
		Req: &t_api.Request{
			Head: map[string]string{
				"id":       "ClaimTaskResume",
				"name":     "task.acquire",
				"protocol": "http",
			},
			Data: &t_api.TaskAcquireRequest{
				Id:        "foo",
				Counter:   2,
				ProcessId: "bar",
				Ttl:       1,
			},
		},
		Res: &t_api.Response{
			Status: t_api.StatusCreated,
			Payload: &t_api.TaskAcquireResponse{
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
	},
	{
		Name: "ClaimTaskTtlTooLarge",
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "POST",
				Path:   "tasks/claim",
				Headers: map[string]string{
					"Request-Id": "ClaimTaskTtlTooLarge",
				},
				Body: []byte(`{
					"id": "foo",
					"processId": "bar",
					"counter": 1,
					"ttl": 9223372036854775808
				}`),
			},
			Res: &httpTestCaseResponse{
				Code: 400,
			},
		},
	},
	{
		Name: "CompleteTask",
		Req: &t_api.Request{
			Head: map[string]string{"id": "CompleteTask", "name": "task.complete"},
			Data: &t_api.TaskCompleteRequest{
				Id:      "foo",
				Counter: 1,
			},
		},
		Res: &t_api.Response{
			Status:  t_api.StatusCreated,
			Payload: &t_api.TaskCompleteResponse{},
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
	},
	{
		Name: "CompleteTaskGet",
		Req: &t_api.Request{
			Head: map[string]string{"id": "CompleteTaskGet", "name": "task.complete"},
			Data: &t_api.TaskCompleteRequest{
				Id:      "foo",
				Counter: 1,
			},
		},
		Res: &t_api.Response{
			Status:  t_api.StatusCreated,
			Payload: &t_api.TaskCompleteResponse{},
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
	},
	{
		Name: "DropTask",
		Req: &t_api.Request{
			Head: map[string]string{"id": "DropTask", "name": "task.release"},
			Data: &t_api.TaskReleaseRequest{
				Id:      "foo",
				Counter: 1,
			},
		},
		Res: &t_api.Response{
			Status:  t_api.StatusCreated,
			Payload: &t_api.TaskReleaseResponse{},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "POST",
				Path:   "tasks/drop",
				Headers: map[string]string{
					"Request-Id": "DropTask",
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
	},
	{
		Name: "DropTaskGet",
		Req: &t_api.Request{
			Head: map[string]string{"id": "DropTaskGet", "name": "task.release"},
			Data: &t_api.TaskReleaseRequest{
				Id:      "foo",
				Counter: 1,
			},
		},
		Res: &t_api.Response{
			Status:  t_api.StatusCreated,
			Payload: &t_api.TaskReleaseResponse{},
		},
		Http: &httpTestCase{
			Req: &httpTestCaseRequest{
				Method: "GET",
				Path:   "tasks/drop/foo/1",
				Headers: map[string]string{
					"Request-Id": "DropTaskGet",
				},
			},
			Res: &httpTestCaseResponse{
				Code: 201,
			},
		},
	},
	{
		Name: "HeartbeatTasks",
		Req: &t_api.Request{
			Head: map[string]string{"id": "HeartbeatTasks", "name": "task.hearbeat"},
			Data: &t_api.TaskHeartbeatRequest{
				ProcessId: "foo",
			},
		},
		Res: &t_api.Response{
			Status: t_api.StatusOK,
			Payload: &t_api.TaskHeartbeatResponse{
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
	},
	{
		Name: "HeartbeatTasksGet",
		Req: &t_api.Request{
			Head: map[string]string{"id": "HeartbeatTasksGet", "name": "task.hearbeat"},
			Data: &t_api.TaskHeartbeatRequest{
				ProcessId: "foo/1",
			},
		},
		Res: &t_api.Response{
			Status: t_api.StatusOK,
			Payload: &t_api.TaskHeartbeatResponse{
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
	},
	{
		Name: "CreatePromiseAndTaskWithTtlZero",
		Req: &t_api.Request{
			Head: map[string]string{"id": "CreatePromiseAndTask", "name": "task.create"},
			Data: &t_api.TaskCreateRequest{
				Promise: &t_api.PromiseCreateRequest{
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
			Status: t_api.StatusCreated,
			Payload: &t_api.TaskCreateResponse{
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
	},
	{
		Name: "CreatePromiseAndTaskWithNegativeTtl",
		Req: &t_api.Request{
			Head: map[string]string{"id": "CreatePromiseAndTask", "name": "task.create"},
			Data: &t_api.TaskCreateRequest{
				Promise: &t_api.PromiseCreateRequest{
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
			Status: t_api.StatusCreated,
			Payload: &t_api.TaskCreateResponse{
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
	},
}

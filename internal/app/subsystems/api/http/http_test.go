package http

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/test"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/schedule"
	"github.com/stretchr/testify/assert"
)

type httpTest struct {
	*test.API
	subsystem api.Subsystem
	errors    chan error
	client    *http.Client
}

func setup() *httpTest {
	api := &test.API{}
	errors := make(chan error)
	subsystem := New(api, &Config{
		Addr:    "127.0.0.1:8888",
		Timeout: 1 * time.Second,
	})

	// start http server
	go subsystem.Start(errors)
	time.Sleep(100 * time.Millisecond)

	return &httpTest{
		API:       api,
		subsystem: subsystem,
		errors:    errors,
		client:    &http.Client{Timeout: 1 * time.Second},
	}
}

func (t *httpTest) teardown() error {
	defer close(t.errors)
	defer t.client.CloseIdleConnections()
	return t.subsystem.Stop()
}

func TestHttpServer(t *testing.T) {
	httpTest := setup()

	for _, tc := range []struct {
		name    string
		path    string
		method  string
		headers map[string]string
		body    []byte
		req     *t_api.Request
		res     *t_api.Response
		status  int
	}{
		{
			name:   "ReadPromise",
			path:   "promises/foo",
			method: "GET",
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
			status: 200,
		},
		{
			name:   "ReadPromiseWithSlash",
			path:   "promises/foo/bar",
			method: "GET",
			req: &t_api.Request{
				Kind: t_api.ReadPromise,
				ReadPromise: &t_api.ReadPromiseRequest{
					Id: "foo/bar",
				},
			},
			res: &t_api.Response{
				Kind: t_api.ReadPromise,
				ReadPromise: &t_api.ReadPromiseResponse{
					Status: t_api.StatusOK,
					Promise: &promise.Promise{
						Id:    "foo/bar",
						State: promise.Pending,
					},
				},
			},
			status: 200,
		},
		{
			name:   "ReadPromiseNotFound",
			path:   "promises/bar",
			method: "GET",
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
			status: 404,
		},
		{
			name:   "SearchPromises",
			path:   "promises?id=*&limit=10",
			method: "GET",
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
			status: 200,
		},
		{
			name:   "SearchPromisesCursor",
			path:   "promises?cursor=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJOZXh0Ijp7ImlkIjoiKiIsInN0YXRlcyI6WyJQRU5ESU5HIl0sImxpbWl0IjoxMCwic29ydElkIjoxMDB9fQ.VbqZxXyDuuOb6o-8CmraefFtDDnmThSopiRT_A-N__0",
			method: "GET",
			req: &t_api.Request{
				Kind: t_api.SearchPromises,
				SearchPromises: &t_api.SearchPromisesRequest{
					Id: "*",
					States: []promise.State{
						promise.Pending,
					},
					Limit:  10,
					SortId: test.Int64ToPointer(100),
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
			status: 200,
		},
		{
			name:   "SearchPromisesPending",
			path:   "promises?id=*&state=pending&limit=10",
			method: "GET",
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
			status: 200,
		},
		{
			name:   "SearchPromisesResolved",
			path:   "promises?id=*&state=resolved&limit=10",
			method: "GET",
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
			status: 200,
		},
		{
			name:   "SearchPromisesRejected",
			path:   "promises?id=*&state=rejected&limit=10",
			method: "GET",
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
			status: 200,
		},
		{
			name:   "SearchPromisesTags",
			path:   "promises?id=*&tags=%7B%22resonate%3Ainvocation%22%3A%22true%22%7D&limit=10",
			method: "GET",
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
			status: 200,
		},
		{
			name:   "SearchPromisesInvalidQuery",
			path:   "promises?id=",
			method: "GET",
			req:    nil,
			res:    nil,
			status: 400,
		},
		{
			name:   "SearchPromisesInvalidLimit",
			path:   "promises?id=*&limit=0",
			method: "GET",
			req:    nil,
			res:    nil,
			status: 400,
		},
		{
			name:   "SearchPromisesInvalidState",
			path:   "promises?id=*&state=x",
			method: "GET",
			req:    nil,
			res:    nil,
			status: 400,
		},
		{
			name:   "SearchPromisesInvalidTags",
			path:   "promises?id=*&tags=x",
			method: "GET",
			req:    nil,
			res:    nil,
			status: 400,
		},
		{
			name:   "CreatePromise",
			path:   "promises",
			method: "POST",
			headers: map[string]string{
				"Idempotency-Key": "bar",
				"Strict":          "true",
			},
			body: []byte(`{
				"id": "foo/bar",
				"param": {
					"headers": {"a":"a","b":"b","c":"c"},
					"data": "cGVuZGluZw=="
				},
				"timeout": 1
			}`),
			req: &t_api.Request{
				Kind: t_api.CreatePromise,
				CreatePromise: &t_api.CreatePromiseRequest{
					Id:             "foo/bar",
					IdempotencyKey: test.IdempotencyKeyToPointer("bar"),
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
						Id:    "foo/bar",
						State: promise.Pending,
					},
				},
			},
			status: 201,
		},
		{
			name:   "CreatePromiseMinimal",
			path:   "promises",
			method: "POST",
			body: []byte(`{
				"id": "foo",
				"timeout": 1
			}`),
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
			status: 201,
		},
		{
			name:   "CancelPromise",
			path:   "promises/foo/bar",
			method: "PATCH",
			headers: map[string]string{
				"Idempotency-Key": "bar",
				"Strict":          "true",
			},
			body: []byte(`{
				"state": "REJECTED_CANCELED", 
				"value": {
					"headers": {"a":"a","b":"b","c":"c"},
					"data": "Y2FuY2Vs"
				}
			}`),
			req: &t_api.Request{
				Kind: t_api.CancelPromise,
				CancelPromise: &t_api.CancelPromiseRequest{
					Id:             "foo/bar",
					IdempotencyKey: test.IdempotencyKeyToPointer("bar"),
					Strict:         true,
					Value: promise.Value{
						Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
						Data:    []byte("cancel"),
					},
				},
			},
			res: &t_api.Response{
				Kind: t_api.CancelPromise,
				CancelPromise: &t_api.CompletePromiseResponse{
					Status: t_api.StatusCreated,
					Promise: &promise.Promise{
						Id:    "foo/bar",
						State: promise.Canceled,
					},
				},
			},
			status: 201,
		},
		{
			name:   "CancelPromiseMinimal",
			path:   "promises/foo",
			method: "PATCH",
			body: []byte(`{
				"state": "REJECTED_CANCELED"
			}`),
			req: &t_api.Request{
				Kind: t_api.CancelPromise,
				CancelPromise: &t_api.CancelPromiseRequest{
					Id:             "foo",
					IdempotencyKey: nil,
					Strict:         false,
					Value: promise.Value{
						Headers: nil,
						Data:    nil,
					},
				},
			},
			res: &t_api.Response{
				Kind: t_api.CancelPromise,
				CancelPromise: &t_api.CompletePromiseResponse{
					Status: t_api.StatusCreated,
					Promise: &promise.Promise{
						Id:    "foo",
						State: promise.Canceled,
					},
				},
			},
			status: 201,
		},
		{
			name:   "ResolvePromise",
			path:   "promises/foo/bar",
			method: "PATCH",
			headers: map[string]string{
				"Idempotency-Key": "bar",
				"Strict":          "true",
			},
			body: []byte(`{
				"state": "RESOLVED", 
				"value": {
					"headers": {"a":"a","b":"b","c":"c"},
					"data": "cmVzb2x2ZQ=="
				}
			}`),
			req: &t_api.Request{
				Kind: t_api.ResolvePromise,
				ResolvePromise: &t_api.ResolvePromiseRequest{
					Id:             "foo/bar",
					IdempotencyKey: test.IdempotencyKeyToPointer("bar"),
					Strict:         true,
					Value: promise.Value{
						Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
						Data:    []byte("resolve"),
					},
				},
			},
			res: &t_api.Response{
				Kind: t_api.ResolvePromise,
				ResolvePromise: &t_api.CompletePromiseResponse{
					Status: t_api.StatusCreated,
					Promise: &promise.Promise{
						Id:    "foo/bar",
						State: promise.Resolved,
					},
				},
			},
			status: 201,
		},
		{
			name:   "ResolvePromiseMinimal",
			path:   "promises/foo",
			method: "PATCH",
			body: []byte(`{
				"state": "RESOLVED" 
			}`),
			req: &t_api.Request{
				Kind: t_api.ResolvePromise,
				ResolvePromise: &t_api.ResolvePromiseRequest{
					Id:             "foo",
					IdempotencyKey: nil,
					Strict:         false,
					Value: promise.Value{
						Headers: nil,
						Data:    nil,
					},
				},
			},
			res: &t_api.Response{
				Kind: t_api.ResolvePromise,
				ResolvePromise: &t_api.CompletePromiseResponse{
					Status: t_api.StatusCreated,
					Promise: &promise.Promise{
						Id:    "foo",
						State: promise.Resolved,
					},
				},
			},
			status: 201,
		},
		{
			name:   "RejectPromise",
			path:   "promises/foo/bar",
			method: "PATCH",
			headers: map[string]string{
				"Idempotency-Key": "bar",
				"Strict":          "true",
			},
			body: []byte(`{
				"state": "REJECTED", 
				"value": {
					"headers": {"a":"a","b":"b","c":"c"},
					"data": "cmVqZWN0"
				}
			}`),
			req: &t_api.Request{
				Kind: t_api.RejectPromise,
				RejectPromise: &t_api.RejectPromiseRequest{
					Id:             "foo/bar",
					IdempotencyKey: test.IdempotencyKeyToPointer("bar"),
					Strict:         true,
					Value: promise.Value{
						Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
						Data:    []byte("reject"),
					},
				},
			},
			res: &t_api.Response{
				Kind: t_api.RejectPromise,
				RejectPromise: &t_api.CompletePromiseResponse{
					Status: t_api.StatusCreated,
					Promise: &promise.Promise{
						Id:    "foo/bar",
						State: promise.Rejected,
					},
				},
			},
			status: 201,
		},
		{
			name:   "RejectPromiseMinimal",
			path:   "promises/foo",
			method: "PATCH",
			body: []byte(`{
				"state": "REJECTED"
			}`),
			req: &t_api.Request{
				Kind: t_api.RejectPromise,
				RejectPromise: &t_api.RejectPromiseRequest{
					Id:             "foo",
					IdempotencyKey: nil,
					Strict:         false,
					Value: promise.Value{
						Headers: nil,
						Data:    nil,
					},
				},
			},
			res: &t_api.Response{
				Kind: t_api.ResolvePromise,
				RejectPromise: &t_api.CompletePromiseResponse{
					Status: t_api.StatusCreated,
					Promise: &promise.Promise{
						Id:    "foo",
						State: promise.Rejected,
					},
				},
			},
			status: 201,
		},
		{
			name:   "ReadSchedule",
			path:   "schedules/foo",
			method: "GET",
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
						Id:             "foo",
						Desc:           "",
						Cron:           "* * * * * *",
						PromiseId:      "foo.{{.timestamp}}",
						PromiseTimeout: 1000000,
					},
				},
			},
			status: 200,
		},
		{
			name:   "CreateSchedule",
			path:   "schedules",
			method: "POST",
			headers: map[string]string{
				"Idempotency-Key": "bar",
			},
			body: []byte(`{
				"id": "foo",
				"desc": "",
				"cron": "* * * * * *",
				"promiseId": "foo.{{.timestamp}}",
				"promiseTimeout": 1000000
			}`),
			req: &t_api.Request{
				Kind: t_api.CreateSchedule,
				CreateSchedule: &t_api.CreateScheduleRequest{
					Id:             "foo",
					IdempotencyKey: test.IdempotencyKeyToPointer("bar"),
					Cron:           "* * * * * *",
					PromiseId:      "foo.{{.timestamp}}",
					PromiseTimeout: 1000000,
				},
			},
			res: &t_api.Response{
				Kind: t_api.CreateSchedule,
				CreateSchedule: &t_api.CreateScheduleResponse{
					Status: t_api.StatusCreated,
					Schedule: &schedule.Schedule{
						Id:             "foo",
						Desc:           "",
						Cron:           "* * * * * *",
						PromiseId:      "foo.{{.timestamp}}",
						PromiseTimeout: 1000000,
					},
				},
			},
			status: 201,
		},
		{
			name:   "DeleteSchedule",
			path:   "schedules/foo",
			method: "DELETE",
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
			status: 204,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			httpTest.Load(t, tc.req, tc.res)

			req, err := http.NewRequest(tc.method, fmt.Sprintf("http://127.0.0.1:8888/%s", tc.path), bytes.NewBuffer(tc.body))
			if err != nil {
				t.Fatal(err)
			}

			// set headers
			req.Header.Set("Content-Type", "application/json")
			for key, val := range tc.headers {
				req.Header.Set(key, val)
			}

			res, err := httpTest.client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()

			body, err := io.ReadAll(res.Body)
			if err != nil {
				t.Fatal(err)
			}

			assert.Equal(t, tc.status, res.StatusCode, string(body))

			// TODO: assert body

			select {
			case err := <-httpTest.errors:
				t.Fatal(err)
			default:
			}
		})
	}

	// stop the server
	if err := httpTest.teardown(); err != nil {
		t.Fatal(err)
	}
}

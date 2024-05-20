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
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/idempotency"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/stretchr/testify/assert"
)

type httpTest struct {
	*test.API
	subsystem api.Subsystem
	errors    chan error
	client    *http.Client
}

func setupAuthWrong() *httpTest {
	api := &test.API{}
	errors := make(chan error)
	subsystem := New(api, &Config{
		Addr:    "127.0.0.1:8888",
		Timeout: 1 * time.Second,
		Auth: CredentialsList{Users: []Credential{
			{Username: "", Password: ""},
			//{Username: "user1", Password: "password1"},
		}},
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

func TestHttpServerWithAuthFailure(t *testing.T) {
	httpTest := setupAuthWrong()

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
			res:    nil,
			status: 401,
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
			res:    nil,
			status: 401,
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
			res:    nil,
			status: 401,
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
					Tags:  map[string]string{},
					Limit: 10,
				},
			},
			res:    nil,
			status: 401,
		},
		{
			name:   "SearchPromisesCursor",
			path:   "promises?cursor=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJOZXh0Ijp7ImlkIjoiKiIsInN0YXRlcyI6WyJQRU5ESU5HIl0sInRhZ3MiOnt9LCJsaW1pdCI6MTAsInNvcnRJZCI6MTAwfX0.XKusWO-Jl4v7QVIwh5Pn3oIElBvtpf0VPOLJkXPvQLk",
			method: "GET",
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
			res:    nil,
			status: 401,
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
					Tags:  map[string]string{},
					Limit: 10,
				},
			},
			res:    nil,
			status: 401,
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
					Tags:  map[string]string{},
					Limit: 10,
				},
			},
			res:    nil,
			status: 401,
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
					Tags:  map[string]string{},
					Limit: 10,
				},
			},
			res:    nil,
			status: 401,
		},
		{
			name:   "SearchPromisesTags",
			path:   "promises?id=*&tags[resonate:invocation]=true&limit=10",
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
			res:    nil,
			status: 401,
		},
		{
			name:   "SearchPromisesInvalidQuery",
			path:   "promises?id=",
			method: "GET",
			req:    nil,
			res:    nil,
			status: 401,
		},
		{
			name:   "SearchPromisesInvalidLimit",
			path:   "promises?id=*&limit=0",
			method: "GET",
			req:    nil,
			res:    nil,
			status: 401,
		},
		{
			name:   "SearchPromisesInvalidState",
			path:   "promises?id=*&state=x",
			method: "GET",
			req:    nil,
			res:    nil,
			status: 401,
		},
		{
			name:   "SearchPromisesInvalidTags",
			path:   "promises?id=*&tags=x",
			method: "GET",
			req:    nil,
			res:    nil,
			status: 401,
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
					IdempotencyKey: util.ToPointer(idempotency.Key("bar")),
					Strict:         true,
					Param: promise.Value{
						Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
						Data:    []byte("pending"),
					},
					Timeout: 1,
				},
			},
			res:    nil,
			status: 401,
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
			res:    nil,
			status: 401,
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
				Kind: t_api.CompletePromise,
				CompletePromise: &t_api.CompletePromiseRequest{
					Id:             "foo/bar",
					IdempotencyKey: util.ToPointer(idempotency.Key("bar")),
					Strict:         true,
					State:          promise.Canceled,
					Value: promise.Value{
						Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
						Data:    []byte("cancel"),
					},
				},
			},
			res:    nil,
			status: 401,
		},
		{
			name:   "CancelPromiseMinimal",
			path:   "promises/foo",
			method: "PATCH",
			body: []byte(`{
				"state": "REJECTED_CANCELED"
			}`),
			req: &t_api.Request{
				Kind: t_api.CompletePromise,
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
			res:    nil,
			status: 401,
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
				Kind: t_api.CompletePromise,
				CompletePromise: &t_api.CompletePromiseRequest{
					Id:             "foo/bar",
					IdempotencyKey: util.ToPointer(idempotency.Key("bar")),
					Strict:         true,
					State:          promise.Resolved,
					Value: promise.Value{
						Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
						Data:    []byte("resolve"),
					},
				},
			},
			res:    nil,
			status: 401,
		},
		{
			name:   "ResolvePromiseMinimal",
			path:   "promises/foo",
			method: "PATCH",
			body: []byte(`{
				"state": "RESOLVED" 
			}`),
			req: &t_api.Request{
				Kind: t_api.CompletePromise,
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
			res:    nil,
			status: 401,
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
				Kind: t_api.CompletePromise,
				CompletePromise: &t_api.CompletePromiseRequest{
					Id:             "foo/bar",
					IdempotencyKey: util.ToPointer(idempotency.Key("bar")),
					Strict:         true,
					State:          promise.Rejected,
					Value: promise.Value{
						Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
						Data:    []byte("reject"),
					},
				},
			},
			res:    nil,
			status: 401,
		},
		{
			name:   "RejectPromiseMinimal",
			path:   "promises/foo",
			method: "PATCH",
			body: []byte(`{
				"state": "REJECTED"
			}`),
			req: &t_api.Request{
				Kind: t_api.CompletePromise,
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
			res:    nil,
			status: 401,
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
			res:    nil,
			status: 401,
		},
		{
			name:   "SearchSchedules",
			path:   "schedules?id=*&limit=10",
			method: "GET",
			req: &t_api.Request{
				Kind: t_api.SearchSchedules,
				SearchSchedules: &t_api.SearchSchedulesRequest{
					Id:    "*",
					Tags:  map[string]string{},
					Limit: 10,
				},
			},
			res:    nil,
			status: 401,
		},
		{
			name:   "SearchSchedulesTags",
			path:   "schedules?id=*&tags[foo]=bar&limit=10",
			method: "GET",
			req: &t_api.Request{
				Kind: t_api.SearchSchedules,
				SearchSchedules: &t_api.SearchSchedulesRequest{
					Id: "*",
					Tags: map[string]string{
						"foo": "bar",
					},
					Limit: 10,
				},
			},
			res:    nil,
			status: 401,
		},
		{
			name:   "SearchSchedulesInvalidQuery",
			path:   "schedules?id=",
			method: "GET",
			req:    nil,
			res:    nil,
			status: 401,
		},
		{
			name:   "SearchSchedulesInvalidLimit",
			path:   "schedules?id=*&limit=0",
			method: "GET",
			req:    nil,
			res:    nil,
			status: 401,
		},
		{
			name:   "SearchSchedulesInvalidTags",
			path:   "schedules?id=*&tags=x",
			method: "GET",
			req:    nil,
			res:    nil,
			status: 401,
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
					IdempotencyKey: util.ToPointer(idempotency.Key("bar")),
					Cron:           "* * * * * *",
					PromiseId:      "foo.{{.timestamp}}",
					PromiseTimeout: 1000000,
				},
			},
			res:    nil,
			status: 401,
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
			res:    nil,
			status: 401,
		},

		// Distributed Locks API
		{
			name:   "AcquireLock",
			path:   "locks/acquire",
			method: "POST",
			body: []byte(`{
				"resourceId": "foo",
				"processId": "bar",
				"executionId": "baz",
				"expiryInMilliseconds": 10000
			}`),
			req: &t_api.Request{
				Kind: t_api.AcquireLock,
				AcquireLock: &t_api.AcquireLockRequest{
					ResourceId:           "foo",
					ProcessId:            "bar",
					ExecutionId:          "baz",
					ExpiryInMilliseconds: 10_000,
				},
			},
			res:    nil,
			status: 401,
		},
		{
			name:   "AcquireLock missing executionIdc",
			path:   "locks/acquire",
			method: "POST",
			body: []byte(`{
				"resourceId": "foo",
				"processId": "bar",
				"executionId": "",
				"expiryInMilliseconds": 10000
			}`),
			req:    nil,
			res:    nil,
			status: 401,
		},
		{
			name:   "HeartbeatLocks",
			path:   "locks/heartbeat",
			method: "POST",
			body: []byte(`{
				"processId": "bar"
			}`),
			req: &t_api.Request{
				Kind: t_api.HeartbeatLocks,
				HeartbeatLocks: &t_api.HeartbeatLocksRequest{
					ProcessId: "bar",
				},
			},
			res:    &t_api.Response{},
			status: 401,
		},
		{
			name:   "HeartbeatLocks missing processId",
			path:   "locks/heartbeat",
			method: "POST",
			body: []byte(`{
				"processId": "",
				"timeout": 1736571600000
			}`),
			req:    nil,
			res:    nil,
			status: 401,
		},
		{
			name:   "ReleaseLock",
			path:   "locks/release",
			method: "POST",
			body: []byte(`{
				"resourceId": "foo",
				"executionId": "baz"
			}`),
			req: &t_api.Request{
				Kind: t_api.ReleaseLock,
				ReleaseLock: &t_api.ReleaseLockRequest{
					ResourceId:  "foo",
					ExecutionId: "baz",
				},
			},
			res:    nil,
			status: 401,
		},
		{
			name:   "ReleaseLock missing resourceId",
			path:   "locks/release",
			method: "POST",
			body: []byte(`{
				"resourceId": "",
				"executionId": "baz"
			}`),
			req:    nil,
			res:    nil,
			status: 401,
		},

		// Tasks API
		{
			name:   "ClaimTask",
			path:   "tasks/claim",
			method: "POST",
			body: []byte(`{
				"taskId": "foo", 
				"counter": 1, 
				"processId": "bar", 
				"executionId": "baz", 
				"expiryInMilliseconds": 10000
			}`),
			req: &t_api.Request{
				Kind: t_api.ClaimTask,
				ClaimTask: &t_api.ClaimTaskRequest{
					TaskId:               "foo",
					Counter:              1,
					ProcessId:            "bar",
					ExecutionId:          "baz",
					ExpiryInMilliseconds: 10_000,
				},
			},
			res:    nil,
			status: 401,
		},
		{
			name:   "CompleteTask",
			path:   "tasks/complete",
			method: "POST",
			body: []byte(`{
				"taskId": "foo", 
				"counter": 1, 
				"executionId": "baz", 
				"state": "RESOLVED", 
				"value": {
					"headers": {"a":"a","b":"b","c":"c"},
					"data": "cGVuZGluZw=="
				}
			}`),
			req: &t_api.Request{
				Kind: t_api.CompleteTask,
				CompleteTask: &t_api.CompleteTaskRequest{
					TaskId:      "foo",
					Counter:     1,
					ExecutionId: "baz",
					State:       promise.Resolved,
					Value: promise.Value{
						Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
						Data:    []byte("pending"),
					},
				},
			},
			res:    nil,
			status: 401,
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


// todo: Distributed Locks API

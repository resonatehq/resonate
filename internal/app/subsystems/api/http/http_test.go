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
	"github.com/resonatehq/resonate/pkg/lock"
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

func setup(auth map[string]string) *httpTest {
	api := &test.API{}
	errors := make(chan error)
	subsystem := New(api, &Config{
		Addr:    "127.0.0.1:8888",
		Auth:    auth,
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
	for _, ts := range []struct {
		name          string
		auth          map[string]string
		reqUsername   string
		reqPassword   string
		statusOveride int
	}{
		{
			name:        "BasicAuthCorrectCredentials",
			auth:        map[string]string{"user1": "pass1"},
			reqUsername: "user1",
			reqPassword: "pass1",
		},
		{
			name:          "BasicAuthIncorrectCredentials",
			auth:          map[string]string{"user1": "pass1"},
			reqUsername:   "user1",
			reqPassword:   "notthepassword",
			statusOveride: 401,
		},
	} {
		// start the server
		httpTest := setup(ts.auth)

		t.Run(ts.name, func(t *testing.T) {
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
					headers: map[string]string{
						"Request-Id": "ReadPromise",
					},
					req: &t_api.Request{
						Kind: t_api.ReadPromise,
						Tags: map[string]string{
							"request_id": "ReadPromise",
							"name":       "ReadPromise",
							"protocol":   "http",
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
					status: 200,
				},
				{
					name:   "ReadPromiseWithSlash",
					path:   "promises/foo/bar",
					method: "GET",
					headers: map[string]string{
						"Request-Id": "ReadPromiseWithSlash",
					},
					req: &t_api.Request{
						Kind: t_api.ReadPromise,
						Tags: map[string]string{
							"request_id": "ReadPromiseWithSlash",
							"name":       "ReadPromise",
							"protocol":   "http",
						},
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
					headers: map[string]string{
						"Request-Id": "ReadPromiseNotFound",
					},
					req: &t_api.Request{
						Kind: t_api.ReadPromise,
						Tags: map[string]string{
							"request_id": "ReadPromiseNotFound",
							"name":       "ReadPromise",
							"protocol":   "http",
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
					status: 404,
				},
				{
					name:   "SearchPromises",
					path:   "promises?id=*&limit=10",
					method: "GET",
					headers: map[string]string{
						"Request-Id": "SearchPromises",
					},
					req: &t_api.Request{
						Kind: t_api.SearchPromises,
						Tags: map[string]string{
							"request_id": "SearchPromises",
							"name":       "SearchPromises",
							"protocol":   "http",
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
							Tags:  map[string]string{},
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
					path:   "promises?cursor=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJOZXh0Ijp7ImlkIjoiKiIsInN0YXRlcyI6WyJQRU5ESU5HIl0sInRhZ3MiOnt9LCJsaW1pdCI6MTAsInNvcnRJZCI6MTAwfX0.XKusWO-Jl4v7QVIwh5Pn3oIElBvtpf0VPOLJkXPvQLk",
					method: "GET",
					headers: map[string]string{
						"Request-Id": "SearchPromisesCursor",
					},
					req: &t_api.Request{
						Kind: t_api.SearchPromises,
						Tags: map[string]string{
							"request_id": "SearchPromisesCursor",
							"name":       "SearchPromises",
							"protocol":   "http",
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
					status: 200,
				},
				{
					name:   "SearchPromisesPending",
					path:   "promises?id=*&state=pending&limit=10",
					method: "GET",
					headers: map[string]string{
						"Request-Id": "SearchPromisesPending",
					},
					req: &t_api.Request{
						Kind: t_api.SearchPromises,
						Tags: map[string]string{
							"request_id": "SearchPromisesPending",
							"name":       "SearchPromises",
							"protocol":   "http",
						},
						SearchPromises: &t_api.SearchPromisesRequest{
							Id: "*",
							States: []promise.State{
								promise.Pending,
							},
							Tags:  map[string]string{},
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
					headers: map[string]string{
						"Request-Id": "SearchPromisesResolved",
					},
					req: &t_api.Request{
						Kind: t_api.SearchPromises,
						Tags: map[string]string{
							"request_id": "SearchPromisesResolved",
							"name":       "SearchPromises",
							"protocol":   "http",
						},
						SearchPromises: &t_api.SearchPromisesRequest{
							Id: "*",
							States: []promise.State{
								promise.Resolved,
							},
							Tags:  map[string]string{},
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
					headers: map[string]string{
						"Request-Id": "SearchPromisesRejected",
					},
					req: &t_api.Request{
						Kind: t_api.SearchPromises,
						Tags: map[string]string{
							"request_id": "SearchPromisesRejected",
							"name":       "SearchPromises",
							"protocol":   "http",
						},
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
					path:   "promises?id=*&tags[resonate:invocation]=true&limit=10",
					method: "GET",
					headers: map[string]string{
						"Request-Id": "SearchPromisesTags",
					},
					req: &t_api.Request{
						Kind: t_api.SearchPromises,
						Tags: map[string]string{
							"request_id": "SearchPromisesTags",
							"name":       "SearchPromises",
							"protocol":   "http",
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
						"Request-Id":      "CreatePromise",
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
						Tags: map[string]string{
							"request_id": "CreatePromise",
							"name":       "CreatePromise",
							"protocol":   "http",
						},
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
					headers: map[string]string{
						"Request-Id": "CreatePromiseMinimal",
					},
					body: []byte(`{
						"id": "foo",
						"timeout": 1
					}`),
					req: &t_api.Request{
						Kind: t_api.CreatePromise,
						Tags: map[string]string{
							"request_id": "CreatePromiseMinimal",
							"name":       "CreatePromise",
							"protocol":   "http",
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
					status: 201,
				},
				{
					name:   "CancelPromise",
					path:   "promises/foo/bar",
					method: "PATCH",
					headers: map[string]string{
						"Request-Id":      "CancelPromise",
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
						Tags: map[string]string{
							"request_id": "CancelPromise",
							"name":       "CompletePromise",
							"protocol":   "http",
						},
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
					res: &t_api.Response{
						Kind: t_api.CompletePromise,
						CompletePromise: &t_api.CompletePromiseResponse{
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
					headers: map[string]string{
						"Request-Id": "CancelPromiseMinimal",
					},
					body: []byte(`{
						"state": "REJECTED_CANCELED"
					}`),
					req: &t_api.Request{
						Kind: t_api.CompletePromise,
						Tags: map[string]string{
							"request_id": "CancelPromiseMinimal",
							"name":       "CompletePromise",
							"protocol":   "http",
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
					status: 201,
				},
				{
					name:   "ResolvePromise",
					path:   "promises/foo/bar",
					method: "PATCH",
					headers: map[string]string{
						"Request-Id":      "ResolvePromise",
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
						Tags: map[string]string{
							"request_id": "ResolvePromise",
							"name":       "CompletePromise",
							"protocol":   "http",
						},
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
					res: &t_api.Response{
						Kind: t_api.CompletePromise,
						CompletePromise: &t_api.CompletePromiseResponse{
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
					headers: map[string]string{
						"Request-Id": "ResolvePromiseMinimal",
					},
					body: []byte(`{
						"state": "RESOLVED"
					}`),
					req: &t_api.Request{
						Kind: t_api.CompletePromise,
						Tags: map[string]string{
							"request_id": "ResolvePromiseMinimal",
							"name":       "CompletePromise",
							"protocol":   "http",
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
					status: 201,
				},
				{
					name:   "RejectPromise",
					path:   "promises/foo/bar",
					method: "PATCH",
					headers: map[string]string{
						"Request-Id":      "RejectPromise",
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
						Tags: map[string]string{
							"request_id": "RejectPromise",
							"name":       "CompletePromise",
							"protocol":   "http",
						},
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
					res: &t_api.Response{
						Kind: t_api.CompletePromise,
						CompletePromise: &t_api.CompletePromiseResponse{
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
					headers: map[string]string{
						"Request-Id": "RejectPromiseMinimal",
					},
					body: []byte(`{
						"state": "REJECTED"
					}`),
					req: &t_api.Request{
						Kind: t_api.CompletePromise,
						Tags: map[string]string{
							"request_id": "RejectPromiseMinimal",
							"name":       "CompletePromise",
							"protocol":   "http",
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
					status: 201,
				},
				{
					name:   "ReadSchedule",
					path:   "schedules/foo",
					method: "GET",
					headers: map[string]string{
						"Request-Id": "ReadSchedule",
					},
					req: &t_api.Request{
						Kind: t_api.ReadSchedule,
						Tags: map[string]string{
							"request_id": "ReadSchedule",
							"name":       "ReadSchedule",
							"protocol":   "http",
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
								Id:             "foo",
								Description:    "",
								Cron:           "* * * * * *",
								PromiseId:      "foo.{{.timestamp}}",
								PromiseTimeout: 1000000,
							},
						},
					},
					status: 200,
				},
				{
					name:   "SearchSchedules",
					path:   "schedules?id=*&limit=10",
					method: "GET",
					headers: map[string]string{
						"Request-Id": "SearchSchedules",
					},
					req: &t_api.Request{
						Kind: t_api.SearchSchedules,
						Tags: map[string]string{
							"request_id": "SearchSchedules",
							"name":       "SearchSchedules",
							"protocol":   "http",
						},
						SearchSchedules: &t_api.SearchSchedulesRequest{
							Id:    "*",
							Tags:  map[string]string{},
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
					status: 200,
				},
				{
					name:   "SearchSchedulesTags",
					path:   "schedules?id=*&tags[foo]=bar&limit=10",
					method: "GET",
					headers: map[string]string{
						"Request-Id": "SearchSchedulesTags",
					},
					req: &t_api.Request{
						Kind: t_api.SearchSchedules,
						Tags: map[string]string{
							"request_id": "SearchSchedulesTags",
							"name":       "SearchSchedules",
							"protocol":   "http",
						},
						SearchSchedules: &t_api.SearchSchedulesRequest{
							Id: "*",
							Tags: map[string]string{
								"foo": "bar",
							},
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
					status: 200,
				},
				{
					name:   "SearchSchedulesInvalidQuery",
					path:   "schedules?id=",
					method: "GET",
					req:    nil,
					res:    nil,
					status: 400,
				},
				{
					name:   "SearchSchedulesInvalidLimit",
					path:   "schedules?id=*&limit=0",
					method: "GET",
					req:    nil,
					res:    nil,
					status: 400,
				},
				{
					name:   "SearchSchedulesInvalidTags",
					path:   "schedules?id=*&tags=x",
					method: "GET",
					req:    nil,
					res:    nil,
					status: 400,
				},
				{
					name:   "CreateSchedule",
					path:   "schedules",
					method: "POST",
					headers: map[string]string{
						"Request-Id":      "CreateSchedule",
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
						Tags: map[string]string{
							"request_id": "CreateSchedule",
							"name":       "CreateSchedule",
							"protocol":   "http",
						},
						CreateSchedule: &t_api.CreateScheduleRequest{
							Id:             "foo",
							IdempotencyKey: util.ToPointer(idempotency.Key("bar")),
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
								Description:    "",
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
					headers: map[string]string{
						"Request-Id": "DeleteSchedule",
					},
					req: &t_api.Request{
						Kind: t_api.DeleteSchedule,
						Tags: map[string]string{
							"request_id": "DeleteSchedule",
							"name":       "DeleteSchedule",
							"protocol":   "http",
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
					status: 204,
				},

				// Distributed Locks API
				{
					name:   "AcquireLock",
					path:   "locks/acquire",
					method: "POST",
					headers: map[string]string{
						"Request-Id": "AcquireLock",
					},
					body: []byte(`{
						"resourceId": "foo",
						"processId": "bar",
						"executionId": "baz",
						"expiryInMilliseconds": 10000
					}`),
					req: &t_api.Request{
						Kind: t_api.AcquireLock,
						Tags: map[string]string{
							"request_id": "AcquireLock",
							"name":       "AcquireLock",
							"protocol":   "http",
						},
						AcquireLock: &t_api.AcquireLockRequest{
							ResourceId:           "foo",
							ProcessId:            "bar",
							ExecutionId:          "baz",
							ExpiryInMilliseconds: 10_000,
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
								ExpiryInMilliseconds: 10_000,
							},
						},
					},
					status: 201,
				},
				{
					name:   "AcquireLockNoExecutionId",
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
					status: 400,
				},
				{
					name:   "ReleaseLock",
					path:   "locks/release",
					method: "POST",
					headers: map[string]string{
						"Request-Id": "ReleaseLock",
					},
					body: []byte(`{
						"resourceId": "foo",
						"executionId": "baz"
					}`),
					req: &t_api.Request{
						Kind: t_api.ReleaseLock,
						Tags: map[string]string{
							"request_id": "ReleaseLock",
							"name":       "ReleaseLock",
							"protocol":   "http",
						},
						ReleaseLock: &t_api.ReleaseLockRequest{
							ResourceId:  "foo",
							ExecutionId: "baz",
						},
					},
					res: &t_api.Response{
						Kind: t_api.ReleaseLock,
						ReleaseLock: &t_api.ReleaseLockResponse{
							Status: t_api.StatusNoContent,
						},
					},
					status: 204,
				},
				{
					name:   "ReleaseLockNoResourceId",
					path:   "locks/release",
					method: "POST",
					body: []byte(`{
						"resourceId": "",
						"executionId": "baz"
					}`),
					req:    nil,
					res:    nil,
					status: 400,
				},
				{
					name:   "HeartbeatLocks",
					path:   "locks/heartbeat",
					method: "POST",
					headers: map[string]string{
						"Request-Id": "HeartbeatLocks",
					},
					body: []byte(`{
						"processId": "bar"
					}`),
					req: &t_api.Request{
						Kind: t_api.HeartbeatLocks,
						Tags: map[string]string{
							"request_id": "HeartbeatLocks",
							"name":       "HeartbeatLocks",
							"protocol":   "http",
						},
						HeartbeatLocks: &t_api.HeartbeatLocksRequest{
							ProcessId: "bar",
						},
					},
					res: &t_api.Response{
						Kind: t_api.HeartbeatLocks,
						HeartbeatLocks: &t_api.HeartbeatLocksResponse{
							Status:        t_api.StatusOK,
							LocksAffected: 0,
						},
					},
					status: 200,
				},
				{
					name:   "HeartbeatLocksNoProcessId",
					path:   "locks/heartbeat",
					method: "POST",
					body: []byte(`{
						"processId": "",
						"timeout": 1736571600000
					}`),
					req:    nil,
					res:    nil,
					status: 400,
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

					// set authorization
					if ts.auth != nil {
						req.SetBasicAuth(ts.reqUsername, ts.reqPassword)
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

					// apply override status if applicable
					status := tc.status
					if ts.statusOveride != 0 {
						status = ts.statusOveride
					}

					assert.Equal(t, status, res.StatusCode, string(body))

					// TODO: assert body

					select {
					case err := <-httpTest.errors:
						t.Fatal(err)
					default:
					}
				})
			}
		})

		// stop the server
		if err := httpTest.teardown(); err != nil {
			t.Fatal(err)
		}
	}
}

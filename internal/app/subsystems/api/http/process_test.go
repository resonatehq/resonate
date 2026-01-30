package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/test"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/message"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/schedule"
	"github.com/resonatehq/resonate/pkg/task"
	"github.com/stretchr/testify/assert"
)

func setupProcess() (*httpTest, error) {
	api := &test.API{}
	metrics := metrics.New(prometheus.NewRegistry())
	errors := make(chan error)
	subsystem, err := New(api, metrics, &Config{
		Addr:          ":0",
		Timeout:       1 * time.Second,
		TaskFrequency: 1 * time.Minute,
	})

	if err != nil {
		return nil, err
	}

	go subsystem.Start(errors)
	time.Sleep(100 * time.Millisecond)

	return &httpTest{
		API:       api,
		subsystem: subsystem,
		errors:    errors,
		client:    &http.Client{Timeout: 1 * time.Second},
	}, nil
}

type processTestCase struct {
	Name         string
	ReqBody      Req
	Expected     *t_api.Request
	Response     *t_api.Response
	HttpCode     int
	ExpectedBody string // JSON string for comparison
}

var processTestCases = []*processTestCase{
	// Promise Get
	{
		Name: "PromiseGet",
		ReqBody: Req{
			Kind: "promise.get",
			Head: Head{CorrId: "test-corr-1"},
			Data: json.RawMessage(`{"id": "foo"}`),
		},
		Expected: &t_api.Request{
			Head: map[string]string{"id": "test-corr-1", "name": "promise.get", "protocol": "http"},
			Data: &t_api.PromiseGetRequest{Id: "foo"},
		},
		Response: &t_api.Response{
			Status: t_api.StatusOK,
			Data: &t_api.PromiseGetResponse{
				Promise: &promise.Promise{
					Id:        "foo",
					State:     promise.Pending,
					CreatedOn: util.ToPointer(int64(1000)),
				},
			},
		},
		HttpCode:     200,
		ExpectedBody: `{"kind":"promise.get","head":{"corrId":"test-corr-1","status":200},"data":{"promise":{"id":"foo","state":"PENDING","param":{},"value":{},"timeoutAt":0,"createdAt":1000}}}`,
	},
	// Promise Create
	{
		Name: "PromiseCreate",
		ReqBody: Req{
			Kind: "promise.create",
			Head: Head{CorrId: "test-corr-2"},
			Data: json.RawMessage(`{"id": "bar", "timeoutAt": 1000}`),
		},
		Expected: &t_api.Request{
			Head: map[string]string{"id": "test-corr-2", "name": "promise.create", "protocol": "http"},
			Data: &t_api.PromiseCreateRequest{
				Id:      "bar",
				Timeout: 1000,
			},
		},
		Response: &t_api.Response{
			Status: t_api.StatusCreated,
			Data: &t_api.PromiseCreateResponse{
				Promise: &promise.Promise{
					Id:        "bar",
					State:     promise.Pending,
					Timeout:   1000,
					CreatedOn: util.ToPointer(int64(500)),
				},
			},
		},
		HttpCode:     200,
		ExpectedBody: `{"kind":"promise.create","head":{"corrId":"test-corr-2","status":201},"data":{"promise":{"id":"bar","state":"PENDING","param":{},"value":{},"timeoutAt":1000,"createdAt":500}}}`,
	},
	// Promise Create with Param and Tags
	{
		Name: "PromiseCreateWithParamAndTags",
		ReqBody: Req{
			Kind: "promise.create",
			Head: Head{CorrId: "test-corr-3"},
			Data: json.RawMessage(`{"id": "baz", "timeoutAt": 2000, "param": {"headers": {"Content-Type": "application/json"}, "data": "dGVzdA=="}, "tags": {"env": "test"}}`),
		},
		Expected: &t_api.Request{
			Head: map[string]string{"id": "test-corr-3", "name": "promise.create", "protocol": "http"},
			Data: &t_api.PromiseCreateRequest{
				Id:      "baz",
				Timeout: 2000,
				Param: promise.Value{
					Headers: map[string]string{"Content-Type": "application/json"},
					Data:    []byte("test"),
				},
				Tags: map[string]string{"env": "test"},
			},
		},
		Response: &t_api.Response{
			Status: t_api.StatusCreated,
			Data: &t_api.PromiseCreateResponse{
				Promise: &promise.Promise{
					Id:      "baz",
					State:   promise.Pending,
					Timeout: 2000,
					Param: promise.Value{
						Headers: map[string]string{"Content-Type": "application/json"},
						Data:    []byte("test"),
					},
					Tags:      map[string]string{"env": "test"},
					CreatedOn: util.ToPointer(int64(100)),
				},
			},
		},
		HttpCode:     200,
		ExpectedBody: `{"kind":"promise.create","head":{"corrId":"test-corr-3","status":201},"data":{"promise":{"id":"baz","state":"PENDING","param":{"headers":{"Content-Type":"application/json"},"data":"dGVzdA=="},"value":{},"tags":{"env":"test"},"timeoutAt":2000,"createdAt":100}}}`,
	},
	// Promise Settle (Resolve)
	{
		Name: "PromiseSettle",
		ReqBody: Req{
			Kind: "promise.settle",
			Head: Head{CorrId: "test-corr-4"},
			Data: json.RawMessage(`{"id": "foo", "state": "RESOLVED"}`),
		},
		Expected: &t_api.Request{
			Head: map[string]string{"id": "test-corr-4", "name": "promise.complete", "protocol": "http"},
			Data: &t_api.PromiseCompleteRequest{
				Id:    "foo",
				State: promise.Resolved,
			},
		},
		Response: &t_api.Response{
			Status: t_api.StatusOK,
			Data: &t_api.PromiseCompleteResponse{
				Promise: &promise.Promise{
					Id:          "foo",
					State:       promise.Resolved,
					CreatedOn:   util.ToPointer(int64(100)),
					CompletedOn: util.ToPointer(int64(200)),
				},
			},
		},
		HttpCode:     200,
		ExpectedBody: `{"kind":"promise.settle","head":{"corrId":"test-corr-4","status":200},"data":{"promise":{"id":"foo","state":"RESOLVED","param":{},"value":{},"timeoutAt":0,"createdAt":100,"settledAt":200}}}`,
	},
	// Promise Register
	{
		Name: "PromiseRegister",
		ReqBody: Req{
			Kind: "promise.register",
			Head: Head{CorrId: "test-corr-register"},
			Data: json.RawMessage(`{"awaiter": "awaiter-promise", "awaited": "awaited-promise"}`),
		},
		Expected: &t_api.Request{
			Head: map[string]string{"id": "test-corr-register", "name": "promise.register", "protocol": "http"},
			Data: &t_api.PromiseRegisterRequest{
				Awaiter: "awaiter-promise",
				Awaited: "awaited-promise",
			},
		},
		Response: &t_api.Response{
			Status: t_api.StatusOK,
			Data: &t_api.PromiseRegisterResponse{
				Promise: &promise.Promise{
					Id:        "awaited-promise",
					State:     promise.Pending,
					CreatedOn: util.ToPointer(int64(100)),
				},
			},
		},
		HttpCode:     200,
		ExpectedBody: `{"kind":"promise.register","head":{"corrId":"test-corr-register","status":200},"data":{"promise":{"id":"awaited-promise","state":"PENDING","param":{},"value":{},"timeoutAt":0,"createdAt":100}}}`,
	},
	// Promise Subscribe
	{
		Name: "PromiseSubscribe",
		ReqBody: Req{
			Kind: "promise.subscribe",
			Head: Head{CorrId: "test-corr-subscribe"},
			Data: json.RawMessage(`{"awaited": "awaited-promise", "address": "http://localhost:8080/callback"}`),
		},
		Expected: &t_api.Request{
			Head: map[string]string{"id": "test-corr-subscribe", "name": "promise.subscribe", "protocol": "http"},
			Data: &t_api.PromiseSubscribeRequest{
				Awaited: "awaited-promise",
				Address: "http://localhost:8080/callback",
			},
		},
		Response: &t_api.Response{
			Status: t_api.StatusOK,
			Data: &t_api.PromiseSubscribeResponse{
				Promise: &promise.Promise{
					Id:        "awaited-promise",
					State:     promise.Pending,
					CreatedOn: util.ToPointer(int64(200)),
				},
			},
		},
		HttpCode:     200,
		ExpectedBody: `{"kind":"promise.subscribe","head":{"corrId":"test-corr-subscribe","status":200},"data":{"promise":{"id":"awaited-promise","state":"PENDING","param":{},"value":{},"timeoutAt":0,"createdAt":200}}}`,
	},
	// Task Create
	{
		Name: "TaskCreate",
		ReqBody: Req{
			Kind: "task.create",
			Head: Head{CorrId: "test-corr-5"},
			Data: json.RawMessage(`{"pid": "worker-1", "ttl": 5000, "action": {"id": "task-promise-1", "timeoutAt": 10000}}`),
		},
		Expected: &t_api.Request{
			Head: map[string]string{"id": "test-corr-5", "name": "task.create", "protocol": "http"},
			Data: &t_api.TaskCreateRequest{
				Promise: &t_api.PromiseCreateRequest{
					Id:      "task-promise-1",
					Timeout: 10000,
				},
				Task: &t_api.CreateTaskRequest{
					PromiseId: "task-promise-1",
					ProcessId: "worker-1",
					Ttl:       5000,
					Timeout:   10000,
				},
			},
		},
		Response: &t_api.Response{
			Status: t_api.StatusCreated,
			Data: &t_api.TaskCreateResponse{
				Task: &task.Task{
					Id:      "task-promise-1",
					Counter: 1,
				},
				Promise: &promise.Promise{
					Id:        "task-promise-1",
					State:     promise.Pending,
					Timeout:   10000,
					CreatedOn: util.ToPointer(int64(100)),
				},
			},
		},
		HttpCode:     200,
		ExpectedBody: `{"kind":"task.create","head":{"corrId":"test-corr-5","status":201},"data":{"promise":{"id":"task-promise-1","state":"PENDING","param":{},"value":{},"timeoutAt":10000,"createdAt":100},"task":{"id":"task-promise-1","version":1}}}`,
	},
	// Task Acquire (invoke case)
	{
		Name: "TaskAcquireInvoke",
		ReqBody: Req{
			Kind: "task.acquire",
			Head: Head{CorrId: "test-corr-6"},
			Data: json.RawMessage(`{"id": "task-1", "version": 1, "pid": "worker-1", "ttl": 5000}`),
		},
		Expected: &t_api.Request{
			Head: map[string]string{"id": "test-corr-6", "name": "task.acquire", "protocol": "http"},
			Data: &t_api.TaskAcquireRequest{
				Id:        "task-1",
				Counter:   1,
				ProcessId: "worker-1",
				Ttl:       5000,
			},
		},
		Response: &t_api.Response{
			Status: t_api.StatusOK,
			Data: &t_api.TaskAcquireResponse{
				LeafPromise: &promise.Promise{
					Id:        "task-1",
					State:     promise.Pending,
					CreatedOn: util.ToPointer(int64(100)),
				},
				RootPromise: &promise.Promise{
					Id:        "task-1",
					State:     promise.Pending,
					CreatedOn: util.ToPointer(int64(100)),
				},
				Task: &task.Task{
					Mesg: &message.Mesg{
						Type: "invoke",
					},
				},
			},
		},
		HttpCode:     200,
		ExpectedBody: `{"kind":"task.acquire","head":{"corrId":"test-corr-6","status":200},"data":{"data":{"preload": [], "promise":{"id":"task-1","state":"PENDING","param":{},"value":{},"timeoutAt":0,"createdAt":100}},"kind":"invoke"}}`,
	},
	// Task Acquire (resume case)
	{
		Name: "TaskAcquireResume",
		ReqBody: Req{
			Kind: "task.acquire",
			Head: Head{CorrId: "test-corr-7"},
			Data: json.RawMessage(`{"id": "task-2", "version": 1, "pid": "worker-1", "ttl": 5000}`),
		},
		Expected: &t_api.Request{
			Head: map[string]string{"id": "test-corr-7", "name": "task.acquire", "protocol": "http"},
			Data: &t_api.TaskAcquireRequest{
				Id:        "task-2",
				Counter:   1,
				ProcessId: "worker-1",
				Ttl:       5000,
			},
		},
		Response: &t_api.Response{
			Status: t_api.StatusOK,
			Data: &t_api.TaskAcquireResponse{
				LeafPromise: &promise.Promise{
					Id:        "child-promise",
					State:     promise.Resolved,
					CreatedOn: util.ToPointer(int64(50)),
				},
				RootPromise: &promise.Promise{
					Id:        "root-promise",
					State:     promise.Pending,
					CreatedOn: util.ToPointer(int64(100)),
				},
				Task: &task.Task{
					Mesg: &message.Mesg{
						Type: "resume",
					},
				},
			},
		},
		HttpCode:     200,
		ExpectedBody: `{"kind":"task.acquire","head":{"corrId":"test-corr-7","status":200},"data":{"kind": "resume", "data":{"preload": [], "promise":{"id":"root-promise","state":"PENDING","param":{},"value":{},"timeoutAt":0,"createdAt":100}}}}`,
	},
	// Task Release
	{
		Name: "TaskRelease",
		ReqBody: Req{
			Kind: "task.release",
			Head: Head{CorrId: "test-corr-8"},
			Data: json.RawMessage(`{"id": "task-1", "version": 1}`),
		},
		Expected: &t_api.Request{
			Head: map[string]string{"id": "test-corr-8", "name": "task.release", "protocol": "http"},
			Data: &t_api.TaskReleaseRequest{
				Id:      "task-1",
				Counter: 1,
			},
		},
		Response: &t_api.Response{
			Status: t_api.StatusOK,
			Data:   &t_api.TaskReleaseResponse{},
		},
		HttpCode:     200,
		ExpectedBody: `{"kind":"task.release","head":{"corrId":"test-corr-8","status":200}}`,
	},
	// Task Fulfill
	{
		Name: "TaskFulfill",
		ReqBody: Req{
			Kind: "task.fulfill",
			Head: Head{CorrId: "test-corr-fulfill"},
			Data: json.RawMessage(`{"id": "task-1", "version": 1, "action": {"id": "task-promise-1", "state": "RESOLVED", "value": {"headers": {"Content-Type": "application/json"}, "data": "cmVzdWx0"}}}`),
		},
		Expected: &t_api.Request{
			Head: map[string]string{"id": "test-corr-fulfill", "name": "task.fulfill", "protocol": "http"},
			Data: &t_api.TaskFulfillRequest{
				Id:      "task-1",
				Version: 1,
				Action: t_api.PromiseCompleteRequest{
					Id:    "task-promise-1",
					State: promise.Resolved,
					Value: promise.Value{
						Headers: map[string]string{"Content-Type": "application/json"},
						Data:    []byte("result"),
					},
				},
			},
		},
		Response: &t_api.Response{
			Status: t_api.StatusOK,
			Data: &t_api.TaskFulfillResponse{
				Promise: &promise.Promise{
					Id:    "task-promise-1",
					State: promise.Resolved,
					Value: promise.Value{
						Headers: map[string]string{"Content-Type": "application/json"},
						Data:    []byte("result"),
					},
					CreatedOn:   util.ToPointer(int64(100)),
					CompletedOn: util.ToPointer(int64(200)),
				},
			},
		},
		HttpCode:     200,
		ExpectedBody: `{"kind":"task.fulfill","head":{"corrId":"test-corr-fulfill","status":200},"data":{"promise":{"id":"task-promise-1","state":"RESOLVED","param":{},"value":{"headers":{"Content-Type":"application/json"},"data":"cmVzdWx0"},"timeoutAt":0,"createdAt":100,"settledAt":200}}}`,
	},
	// Task Fulfill Conflict (already claimed)
	{
		Name: "TaskFulfillConflict",
		ReqBody: Req{
			Kind: "task.fulfill",
			Head: Head{CorrId: "test-corr-fulfill-conflict"},
			Data: json.RawMessage(`{"id": "task-1", "version": 1, "action": {"id": "task-promise-1", "state": "RESOLVED"}}`),
		},
		Expected: &t_api.Request{
			Head: map[string]string{"id": "test-corr-fulfill-conflict", "name": "task.fulfill", "protocol": "http"},
			Data: &t_api.TaskFulfillRequest{
				Id:      "task-1",
				Version: 1,
				Action: t_api.PromiseCompleteRequest{
					Id:    "task-promise-1",
					State: promise.Resolved,
				},
			},
		},
		Response: &t_api.Response{
			Status: t_api.StatusTaskNotClaimed,
		},
		HttpCode:     409,
		ExpectedBody: `{"kind":"task.fulfill","head":{"corrId":"test-corr-fulfill-conflict","status":409},"data":"The task state is invalid"}`,
	},
	// Task Heartbeat
	{
		Name: "TaskHeartbeat",
		ReqBody: Req{
			Kind: "task.heartbeat",
			Head: Head{CorrId: "test-corr-9"},
			Data: json.RawMessage(`{"pid": "worker-1"}`),
		},
		Expected: &t_api.Request{
			Head: map[string]string{"id": "test-corr-9", "name": "task.hearbeat", "protocol": "http"},
			Data: &t_api.TaskHeartbeatRequest{
				ProcessId: "worker-1",
			},
		},
		Response: &t_api.Response{
			Status: t_api.StatusOK,
			Data:   &t_api.TaskHeartbeatResponse{},
		},
		HttpCode:     200,
		ExpectedBody: `{"kind":"task.heartbeat","head":{"corrId":"test-corr-9","status":200}}`,
	},
	// Task Suspend
	{
		Name: "TaskSuspend",
		ReqBody: Req{
			Kind: "task.suspend",
			Head: Head{CorrId: "test-corr-suspend"},
			Data: json.RawMessage(`{"id": "task-1", "version": 1, "actions": [{"awaiter": "promise-1", "awaited": "promise-2"}]}`),
		},
		Expected: &t_api.Request{
			Head: map[string]string{"id": "test-corr-suspend", "name": "task.suspend", "protocol": "http"},
			Data: &t_api.TaskSuspendRequest{
				Id:      "task-1",
				Version: 1,
				Actions: []t_api.PromiseRegisterRequest{
					{Awaiter: "promise-1", Awaited: "promise-2"},
				},
			},
		},
		Response: &t_api.Response{
			Status: t_api.StatusOK,
			Data:   &t_api.TaskSuspendResponse{},
		},
		HttpCode:     200,
		ExpectedBody: `{"kind":"task.suspend","head":{"corrId":"test-corr-suspend","status":200}}`,
	},
	// Task Suspend with multiple actions
	{
		Name: "TaskSuspendMultipleActions",
		ReqBody: Req{
			Kind: "task.suspend",
			Head: Head{CorrId: "test-corr-suspend-multi"},
			Data: json.RawMessage(`{"id": "task-2", "version": 3, "actions": [{"awaiter": "p1", "awaited": "p2"}, {"awaiter": "p1", "awaited": "p3"}, {"awaiter": "p1", "awaited": "p4"}]}`),
		},
		Expected: &t_api.Request{
			Head: map[string]string{"id": "test-corr-suspend-multi", "name": "task.suspend", "protocol": "http"},
			Data: &t_api.TaskSuspendRequest{
				Id:      "task-2",
				Version: 3,
				Actions: []t_api.PromiseRegisterRequest{
					{Awaiter: "p1", Awaited: "p2"},
					{Awaiter: "p1", Awaited: "p3"},
					{Awaiter: "p1", Awaited: "p4"},
				},
			},
		},
		Response: &t_api.Response{
			Status: t_api.StatusOK,
			Data:   &t_api.TaskSuspendResponse{},
		},
		HttpCode:     200,
		ExpectedBody: `{"kind":"task.suspend","head":{"corrId":"test-corr-suspend-multi","status":200}}`,
	},
	// Schedule Get
	{
		Name: "ScheduleGet",
		ReqBody: Req{
			Kind: "schedule.get",
			Head: Head{CorrId: "test-corr-10"},
			Data: json.RawMessage(`{"id": "schedule-1"}`),
		},
		Expected: &t_api.Request{
			Head: map[string]string{"id": "test-corr-10", "name": "schedule.get", "protocol": "http"},
			Data: &t_api.ScheduleGetRequest{Id: "schedule-1"},
		},
		Response: &t_api.Response{
			Status: t_api.StatusOK,
			Data: &t_api.ScheduleGetResponse{
				Schedule: &schedule.Schedule{
					Id:             "schedule-1",
					Cron:           "* * * * *",
					PromiseId:      "promise-template",
					PromiseTimeout: 5000,
					CreatedOn:      100,
					NextRunTime:    200,
				},
			},
		},
		HttpCode:     200,
		ExpectedBody: `{"kind":"schedule.get","head":{"corrId":"test-corr-10","status":200},"data":{"schedule":{"id":"schedule-1","cron":"* * * * *","promiseId":"promise-template","promiseTimeout":5000,"promiseParam":{},"createdAt":100,"nextRunAt":200}}}`,
	},
	// Schedule Create
	{
		Name: "ScheduleCreate",
		ReqBody: Req{
			Kind: "schedule.create",
			Head: Head{CorrId: "test-corr-11"},
			Data: json.RawMessage(`{"id": "schedule-2", "cron": "0 * * * *", "promiseId": "scheduled-promise", "promiseTimeout": 10000}`),
		},
		Expected: &t_api.Request{
			Head: map[string]string{"id": "test-corr-11", "name": "schedule.create", "protocol": "http"},
			Data: &t_api.ScheduleCreateRequest{
				Id:             "schedule-2",
				Cron:           "0 * * * *",
				PromiseId:      "scheduled-promise",
				PromiseTimeout: 10000,
			},
		},
		Response: &t_api.Response{
			Status: t_api.StatusCreated,
			Data: &t_api.ScheduleCreateResponse{
				Schedule: &schedule.Schedule{
					Id:             "schedule-2",
					Cron:           "0 * * * *",
					PromiseId:      "scheduled-promise",
					PromiseTimeout: 10000,
					CreatedOn:      300,
					NextRunTime:    400,
				},
			},
		},
		HttpCode:     200,
		ExpectedBody: `{"kind":"schedule.create","head":{"corrId":"test-corr-11","status":201},"data":{"schedule":{"id":"schedule-2","cron":"0 * * * *","promiseId":"scheduled-promise","promiseTimeout":10000,"promiseParam":{},"createdAt":300,"nextRunAt":400}}}`,
	},
	// Schedule Delete
	{
		Name: "ScheduleDelete",
		ReqBody: Req{
			Kind: "schedule.delete",
			Head: Head{CorrId: "test-corr-12"},
			Data: json.RawMessage(`{"id": "schedule-1"}`),
		},
		Expected: &t_api.Request{
			Head: map[string]string{"id": "test-corr-12", "name": "schedule.delete", "protocol": "http"},
			Data: &t_api.ScheduleDeleteRequest{Id: "schedule-1"},
		},
		Response: &t_api.Response{
			Status: t_api.StatusNoContent,
			Data:   &t_api.ScheduleDeleteResponse{},
		},
		HttpCode:     200,
		ExpectedBody: `{"kind":"schedule.delete","head":{"corrId":"test-corr-12","status":204}}`,
	},
	// Authorization header forwarding
	{
		Name: "PromiseGetWithAuth",
		ReqBody: Req{
			Kind: "promise.get",
			Head: Head{CorrId: "test-corr-auth", Auth: "my-token"},
			Data: json.RawMessage(`{"id": "foo"}`),
		},
		Expected: &t_api.Request{
			Head: map[string]string{"id": "test-corr-auth", "name": "promise.get", "protocol": "http", "authorization": "my-token"},
			Data: &t_api.PromiseGetRequest{Id: "foo"},
		},
		Response: &t_api.Response{
			Status: t_api.StatusOK,
			Data: &t_api.PromiseGetResponse{
				Promise: &promise.Promise{
					Id:        "foo",
					State:     promise.Pending,
					CreatedOn: util.ToPointer(int64(1000)),
				},
			},
		},
		HttpCode:     200,
		ExpectedBody: `{"kind":"promise.get","head":{"corrId":"test-corr-auth","status":200},"data":{"promise":{"id":"foo","state":"PENDING","param":{},"value":{},"timeoutAt":0,"createdAt":1000}}}`,
	},
	{
		Name: "ErrorInProcessApi",
		ReqBody: Req{
			Kind: "promise.get",
			Head: Head{CorrId: "test-corr-err"},
			Data: json.RawMessage(`{"id": "foo"}`),
		},
		Expected: &t_api.Request{
			Head: map[string]string{"id": "test-corr-err", "name": "promise.get", "protocol": "http"},
			Data: &t_api.PromiseGetRequest{Id: "foo"},
		},
		Response: &t_api.Response{
			Status: t_api.StatusAIOStoreError,
		},
		HttpCode:     500,
		ExpectedBody: `{"kind":"promise.get","head":{"corrId":"test-corr-err","status":500},"data":"There was an error in the store subsystem"}`,
	},
}

func TestProcess(t *testing.T) {
	httpTest, err := setupProcess()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := httpTest.teardown(); err != nil {
			t.Fatal(err)
		}
	}()

	for _, tc := range processTestCases {
		t.Run(tc.Name, func(t *testing.T) {
			httpTest.Load(t, tc.Expected, tc.Response)

			reqBody, err := json.Marshal(tc.ReqBody)
			if err != nil {
				t.Fatal(err)
			}

			req, err := http.NewRequest("POST", fmt.Sprintf("http://%s/api", httpTest.subsystem.Addr()), bytes.NewBuffer(reqBody))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/json")

			res, err := httpTest.client.Do(req)
			if err != nil {
				t.Fatal(err)
			}

			defer util.DeferAndLog(res.Body.Close)

			body, err := io.ReadAll(res.Body)
			if err != nil {
				t.Fatal(err)
			}

			assert.Equal(t, tc.HttpCode, res.StatusCode, string(body))
			assert.JSONEq(t, tc.ExpectedBody, string(body), string(body))

			select {
			case err := <-httpTest.errors:
				t.Fatal(err)
			default:
			}
		})
	}
}

func TestProcessValidationErrors(t *testing.T) {
	httpTest, err := setupProcess()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := httpTest.teardown(); err != nil {
			t.Fatal(err)
		}
	}()

	testCases := []struct {
		Name         string
		ReqBody      string
		ExpectedKind string
		HttpCode     int
	}{
		{
			Name:         "MissingKind",
			ReqBody:      `{"head": {"corrId": "test"}, "data": {"id": "foo"}}`,
			ExpectedKind: "error",
			HttpCode:     400,
		},
		{
			Name:         "MissingData",
			ReqBody:      `{"kind": "promise.get", "head": {"corrId": "test"}}`,
			ExpectedKind: "promise.get",
			HttpCode:     400,
		},
		{
			Name:         "UnknownKind",
			ReqBody:      `{"kind": "unknown.kind", "head": {"corrId": "test"}, "data": {"id": "foo"}}`,
			ExpectedKind: "unknown.kind",
			HttpCode:     400,
		},
		{
			Name:         "PromiseGetMissingId",
			ReqBody:      `{"kind": "promise.get", "head": {"corrId": "test"}, "data": {}}`,
			ExpectedKind: "promise.get",
			HttpCode:     400,
		},
		{
			Name:         "PromiseCreateMissingId",
			ReqBody:      `{"kind": "promise.create", "head": {"corrId": "test"}, "data": {"timeoutAt": 1000}}`,
			ExpectedKind: "promise.create",
			HttpCode:     400,
		},
		{
			Name:         "PromiseCreateMissingTimeout",
			ReqBody:      `{"kind": "promise.create", "head": {"corrId": "test"}, "data": {"id": "foo"}}`,
			ExpectedKind: "promise.create",
			HttpCode:     400,
		},
		{
			Name:         "PromiseSettleMissingState",
			ReqBody:      `{"kind": "promise.settle", "head": {"corrId": "test"}, "data": {"id": "foo"}}`,
			ExpectedKind: "promise.settle",
			HttpCode:     400,
		},
		{
			Name:         "TaskCreateMissingPid",
			ReqBody:      `{"kind": "task.create", "head": {"corrId": "test"}, "data": {"ttl": 1000, "action": {"id": "foo", "timeoutAt": 2000}}}`,
			ExpectedKind: "task.create",
			HttpCode:     400,
		},
		{
			Name:         "TaskAcquireMissingVersion",
			ReqBody:      `{"kind": "task.acquire", "head": {"corrId": "test"}, "data": {"id": "foo", "pid": "worker", "ttl": 1000}}`,
			ExpectedKind: "task.acquire",
			HttpCode:     400,
		},
		{
			Name:         "TaskFulfillMissingId",
			ReqBody:      `{"kind": "task.fulfill", "head": {"corrId": "test"}, "data": {"version": 1, "action": {"id": "foo", "state": "RESOLVED"}}}`,
			ExpectedKind: "task.fulfill",
			HttpCode:     400,
		},
		{
			Name:         "TaskFulfillMissingVersion",
			ReqBody:      `{"kind": "task.fulfill", "head": {"corrId": "test"}, "data": {"id": "task-1", "action": {"id": "foo", "state": "RESOLVED"}}}`,
			ExpectedKind: "task.fulfill",
			HttpCode:     400,
		},
		{
			Name:         "TaskFulfillMissingAction",
			ReqBody:      `{"kind": "task.fulfill", "head": {"corrId": "test"}, "data": {"id": "task-1", "version": 1}}`,
			ExpectedKind: "task.fulfill",
			HttpCode:     400,
		},
		{
			Name:         "TaskSuspendMissingId",
			ReqBody:      `{"kind": "task.suspend", "head": {"corrId": "test"}, "data": {"version": 1, "actions": []}}`,
			ExpectedKind: "task.suspend",
			HttpCode:     400,
		},
		{
			Name:         "TaskSuspendMissingVersion",
			ReqBody:      `{"kind": "task.suspend", "head": {"corrId": "test"}, "data": {"id": "task-1", "actions": []}}`,
			ExpectedKind: "task.suspend",
			HttpCode:     400,
		},
		{
			Name:         "TaskSuspendMissingActions",
			ReqBody:      `{"kind": "task.suspend", "head": {"corrId": "test"}, "data": {"id": "task-1", "version": 1}}`,
			ExpectedKind: "task.suspend",
			HttpCode:     400,
		},
		{
			Name:         "ScheduleCreateMissingCron",
			ReqBody:      `{"kind": "schedule.create", "head": {"corrId": "test"}, "data": {"id": "sched", "promiseId": "prom", "promiseTimeout": 1000}}`,
			ExpectedKind: "schedule.create",
			HttpCode:     400,
		},
		{
			Name:         "PromiseRegisterMissingAwaiter",
			ReqBody:      `{"kind": "promise.register", "head": {"corrId": "test"}, "data": {"awaited": "foo"}}`,
			ExpectedKind: "promise.register",
			HttpCode:     400,
		},
		{
			Name:         "PromiseRegisterMissingAwaited",
			ReqBody:      `{"kind": "promise.register", "head": {"corrId": "test"}, "data": {"awaiter": "foo"}}`,
			ExpectedKind: "promise.register",
			HttpCode:     400,
		},
		{
			Name:         "PromiseSubscribeMissingAwaited",
			ReqBody:      `{"kind": "promise.subscribe", "head": {"corrId": "test"}, "data": {"address": "http://localhost"}}`,
			ExpectedKind: "promise.subscribe",
			HttpCode:     400,
		},
		{
			Name:         "PromiseSubscribeMissingAddress",
			ReqBody:      `{"kind": "promise.subscribe", "head": {"corrId": "test"}, "data": {"awaited": "foo"}}`,
			ExpectedKind: "promise.subscribe",
			HttpCode:     400,
		},
		{
			Name:         "InvalidJson",
			ReqBody:      `{invalid json}`,
			ExpectedKind: "error",
			HttpCode:     400,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			req, err := http.NewRequest("POST", fmt.Sprintf("http://%s/api", httpTest.subsystem.Addr()), bytes.NewBufferString(tc.ReqBody))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/json")

			res, err := httpTest.client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer util.DeferAndLog(res.Body.Close)

			body, err := io.ReadAll(res.Body)
			if err != nil {
				t.Fatal(err)
			}

			assert.Equal(t, tc.HttpCode, res.StatusCode, string(body))

			var actual Res
			if err := json.Unmarshal(body, &actual); err != nil {
				t.Fatalf("failed to unmarshal response: %v, body: %s", err, string(body))
			}
			assert.Equal(t, tc.ExpectedKind, actual.Kind)
		})
	}
}

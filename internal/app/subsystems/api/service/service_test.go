package service

// import (
// 	"fmt"
// 	"testing"

// 	"github.com/resonatehq/resonate/internal/app/subsystems/api/test"
// 	"github.com/resonatehq/resonate/internal/util"
// 	"github.com/resonatehq/resonate/pkg/promise"
// 	"github.com/resonatehq/resonate/pkg/schedule"

// 	"github.com/resonatehq/resonate/internal/kernel/t_api"
// 	"github.com/stretchr/testify/assert"
// )

// type serviceTest struct {
// 	*test.API
// 	service *Service
// }

// func setup() *serviceTest {
// 	api := &test.API{}
// 	service := New(api, "local")

// 	return &serviceTest{
// 		API:     api,
// 		service: service,
// 	}
// }

// func TestReadPromise(t *testing.T) {
// 	serviceTest := setup()

// 	for _, tc := range []struct {
// 		name           string
// 		id             string
// 		req            *t_api.Request
// 		res            *t_api.Response
// 		expectedErrMsg *string
// 	}{
// 		{
// 			name: "ReadPromise",
// 			id:   "foo",
// 			req: &t_api.Request{
// 				Kind: t_api.ReadPromise,
// 				ReadPromise: &t_api.ReadPromiseRequest{
// 					Id: "foo",
// 				},
// 			},
// 			res: &t_api.Response{
// 				Kind: t_api.ReadPromise,
// 				ReadPromise: &t_api.ReadPromiseResponse{
// 					Status: t_api.StatusOK,
// 					Promise: &promise.Promise{
// 						Id:    "foo",
// 						State: promise.Pending,
// 					},
// 				},
// 			},
// 		},
// 		{
// 			name: "ReadPromiseNotFound",
// 			id:   "bar",
// 			req: &t_api.Request{
// 				Kind: t_api.ReadPromise,
// 				ReadPromise: &t_api.ReadPromiseRequest{
// 					Id: "bar",
// 				},
// 			},
// 			res: &t_api.Response{
// 				Kind: t_api.ReadPromise,
// 				ReadPromise: &t_api.ReadPromiseResponse{
// 					Status:  t_api.StatusPromiseNotFound,
// 					Promise: nil,
// 				},
// 			},
// 			expectedErrMsg: util.ToPointer(`{"error":{"code":4040,"message":"The specified promise was not found","details":[{"@type":"RequestError","message":"Request errors are not retryable since they are caused by invalid client requests","domain":"request","metadata":{"url":"https://docs.resonatehq.io/reference/error-codes#4040"}}]}}`),
// 		},
// 	} {
// 		t.Run(tc.name, func(t *testing.T) {
// 			serviceTest.Load(t, tc.req, tc.res)

// 			res, err := serviceTest.service.ReadPromise(tc.id, &Header{})
// 			if err != nil {
// 				assert.Equal(t, *tc.expectedErrMsg, err.Error())
// 				return
// 			}

// 			assert.Equal(t, tc.res.ReadPromise, res)
// 		})
// 	}
// }

// func TestSearchPromises(t *testing.T) {
// 	serviceTest := setup()

// 	for _, tc := range []struct {
// 		name       string
// 		serviceReq *SearchPromiseParams
// 		req        *t_api.Request
// 		res        *t_api.Response
// 	}{
// 		{
// 			name: "SearchPromises",
// 			serviceReq: &SearchPromiseParams{
// 				Id:    util.ToPointer("*"),
// 				Limit: util.ToPointer(10),
// 			},
// 			req: &t_api.Request{
// 				Kind: t_api.SearchPromises,
// 				SearchPromises: &t_api.SearchPromisesRequest{
// 					Id: "*",
// 					States: []promise.State{
// 						promise.Pending,
// 						promise.Resolved,
// 						promise.Rejected,
// 						promise.Timedout,
// 						promise.Canceled,
// 					},
// 					Limit: 10,
// 				},
// 			},
// 			res: &t_api.Response{
// 				Kind: t_api.SearchPromises,
// 				SearchPromises: &t_api.SearchPromisesResponse{
// 					Status:   t_api.StatusOK,
// 					Cursor:   nil,
// 					Promises: []*promise.Promise{},
// 				},
// 			},
// 		},
// 		{
// 			name: "SearchPromisesCursor",
// 			serviceReq: &SearchPromiseParams{
// 				Cursor: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJOZXh0Ijp7ImlkIjoiKiIsInN0YXRlcyI6WyJQRU5ESU5HIl0sImxpbWl0IjoxMCwic29ydElkIjoxMDB9fQ.VbqZxXyDuuOb6o-8CmraefFtDDnmThSopiRT_A-N__0",
// 			},
// 			req: &t_api.Request{
// 				Kind: t_api.SearchPromises,
// 				SearchPromises: &t_api.SearchPromisesRequest{
// 					Id: "*",
// 					States: []promise.State{
// 						promise.Pending,
// 					},
// 					Limit:  10,
// 					SortId: test.Int64ToPointer(100),
// 				},
// 			},
// 			res: &t_api.Response{
// 				Kind: t_api.SearchPromises,
// 				SearchPromises: &t_api.SearchPromisesResponse{
// 					Status: t_api.StatusOK,
// 					Cursor: &t_api.Cursor[t_api.SearchPromisesRequest]{
// 						Next: &t_api.SearchPromisesRequest{
// 							Id: "*",
// 							States: []promise.State{
// 								promise.Pending,
// 								promise.Resolved,
// 								promise.Rejected,
// 								promise.Timedout,
// 								promise.Canceled,
// 							},
// 							Limit:  10,
// 							SortId: test.Int64ToPointer(10),
// 						},
// 					},
// 					Promises: []*promise.Promise{},
// 				},
// 			},
// 		},
// 		{
// 			name: "SearchPromisesPending",
// 			serviceReq: &SearchPromiseParams{
// 				Id:    util.ToPointer("*"),
// 				State: "pending",
// 				Limit: util.ToPointer(10),
// 			},
// 			req: &t_api.Request{
// 				Kind: t_api.SearchPromises,
// 				SearchPromises: &t_api.SearchPromisesRequest{
// 					Id: "*",
// 					States: []promise.State{
// 						promise.Pending,
// 					},
// 					Limit: 10,
// 				},
// 			},
// 			res: &t_api.Response{
// 				Kind: t_api.SearchPromises,
// 				SearchPromises: &t_api.SearchPromisesResponse{
// 					Status:   t_api.StatusOK,
// 					Cursor:   nil,
// 					Promises: []*promise.Promise{},
// 				},
// 			},
// 		},
// 		{
// 			name: "SearchPromisesResolved",
// 			serviceReq: &SearchPromiseParams{
// 				Id:    util.ToPointer("*"),
// 				State: "resolved",
// 				Limit: util.ToPointer(10),
// 			},
// 			req: &t_api.Request{
// 				Kind: t_api.SearchPromises,
// 				SearchPromises: &t_api.SearchPromisesRequest{
// 					Id: "*",
// 					States: []promise.State{
// 						promise.Resolved,
// 					},
// 					Limit: 10,
// 				},
// 			},
// 			res: &t_api.Response{
// 				Kind: t_api.SearchPromises,
// 				SearchPromises: &t_api.SearchPromisesResponse{
// 					Status:   t_api.StatusOK,
// 					Cursor:   nil,
// 					Promises: []*promise.Promise{},
// 				},
// 			},
// 		},
// 		{
// 			name: "SearchPromisesRejected",
// 			serviceReq: &SearchPromiseParams{
// 				Id:    util.ToPointer("*"),
// 				State: "rejected",
// 				Limit: util.ToPointer(10),
// 			},
// 			req: &t_api.Request{
// 				Kind: t_api.SearchPromises,
// 				SearchPromises: &t_api.SearchPromisesRequest{
// 					Id: "*",
// 					States: []promise.State{
// 						promise.Rejected,
// 						promise.Timedout,
// 						promise.Canceled,
// 					},
// 					Limit: 10,
// 				},
// 			},
// 			res: &t_api.Response{
// 				Kind: t_api.SearchPromises,
// 				SearchPromises: &t_api.SearchPromisesResponse{
// 					Status:   t_api.StatusOK,
// 					Cursor:   nil,
// 					Promises: []*promise.Promise{},
// 				},
// 			},
// 		},
// 	} {
// 		t.Run(tc.name, func(t *testing.T) {
// 			serviceTest.Load(t, tc.req, tc.res)
// 			res, err := serviceTest.service.SearchPromises(&Header{}, tc.serviceReq)
// 			if err != nil {
// 				fmt.Println("we are here bro", res.Status)
// 				t.Fatal(err)
// 			}

// 			assert.Equal(t, tc.res.SearchPromises, res)
// 		})
// 	}
// }

// func TestCreatePromise(t *testing.T) {
// 	serviceTest := setup()

// 	for _, tc := range []struct {
// 		name             string
// 		serviceReqHeader *CreatePromiseHeader
// 		serviceReqBody   *promise.Promise
// 		req              *t_api.Request
// 		res              *t_api.Response
// 	}{
// 		{
// 			name: "CreatePromise",
// 			serviceReqHeader: &CreatePromiseHeader{
// 				IdempotencyKey: test.IdempotencyKeyToPointer("bar"),
// 				Strict:         true,
// 			},
// 			serviceReqBody: &promise.Promise{
// 				Id: "foo",
// 				Param: promise.Value{
// 					Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
// 					Data:    []byte("pending"),
// 				},
// 				Timeout: int64(1),
// 			},

// 			req: &t_api.Request{
// 				Kind: t_api.CreatePromise,
// 				CreatePromise: &t_api.CreatePromiseRequest{
// 					Id:             "foo",
// 					IdempotencyKey: test.IdempotencyKeyToPointer("bar"),
// 					Strict:         true,
// 					Param: promise.Value{
// 						Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
// 						Data:    []byte("pending"),
// 					},
// 					Timeout: 1,
// 				},
// 			},
// 			res: &t_api.Response{
// 				Kind: t_api.CreatePromise,
// 				CreatePromise: &t_api.CreatePromiseResponse{
// 					Status: t_api.StatusCreated,
// 					Promise: &promise.Promise{
// 						Id:    "foo",
// 						State: promise.Pending,
// 					},
// 				},
// 			},
// 		},
// 		{
// 			name: "CreatePromiseMinimal",
// 			serviceReqHeader: &CreatePromiseHeader{
// 				IdempotencyKey: nil,
// 				Strict:         false,
// 			},
// 			serviceReqBody: &promise.Promise{
// 				Id: "foo",
// 				Param: promise.Value{
// 					Headers: nil,
// 					Data:    nil,
// 				},
// 				Timeout: int64(1),
// 			},

// 			req: &t_api.Request{
// 				Kind: t_api.CreatePromise,
// 				CreatePromise: &t_api.CreatePromiseRequest{
// 					Id:             "foo",
// 					IdempotencyKey: nil,
// 					Strict:         false,
// 					Param: promise.Value{
// 						Headers: nil,
// 						Data:    nil,
// 					},
// 					Timeout: 1,
// 				},
// 			},
// 			res: &t_api.Response{
// 				Kind: t_api.CreatePromise,
// 				CreatePromise: &t_api.CreatePromiseResponse{
// 					Status: t_api.StatusCreated,
// 					Promise: &promise.Promise{
// 						Id:    "foo",
// 						State: promise.Pending,
// 					},
// 				},
// 			},
// 		},
// 	} {
// 		t.Run(tc.name, func(t *testing.T) {
// 			serviceTest.Load(t, tc.req, tc.res)
// 			res, err := serviceTest.service.CreatePromise(tc.serviceReqHeader, tc.serviceReqBody)
// 			if err != nil {
// 				t.Fatal(err)
// 			}

// 			assert.Equal(t, tc.res.CreatePromise, res)
// 		})
// 	}
// }

// func TestCancelPromise(t *testing.T) {
// 	serviceTest := setup()

// 	for _, tc := range []struct {
// 		name             string
// 		id               string
// 		serviceReqHeader *CompletePromiseHeader
// 		serviceReqBody   *CompletePromiseBody
// 		req              *t_api.Request
// 		res              *t_api.Response
// 	}{
// 		{
// 			name: "CancelPromise",
// 			id:   "foo",
// 			serviceReqHeader: &CompletePromiseHeader{
// 				IdempotencyKey: test.IdempotencyKeyToPointer("bar"),
// 				Strict:         true,
// 			},
// 			serviceReqBody: &CompletePromiseBody{
// 				Value: promise.Value{
// 					Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
// 					Data:    []byte("cancel"),
// 				},
// 			},
// 			req: &t_api.Request{
// 				Kind: t_api.CancelPromise,
// 				CancelPromise: &t_api.CancelPromiseRequest{
// 					Id:             "foo",
// 					IdempotencyKey: test.IdempotencyKeyToPointer("bar"),
// 					Strict:         true,
// 					Value: promise.Value{
// 						Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
// 						Data:    []byte("cancel"),
// 					},
// 				},
// 			},
// 			res: &t_api.Response{
// 				Kind: t_api.CancelPromise,
// 				CancelPromise: &t_api.CompletePromiseResponse{
// 					Status: t_api.StatusCreated,
// 					Promise: &promise.Promise{
// 						Id:    "foo",
// 						State: promise.Canceled,
// 					},
// 				},
// 			},
// 		},
// 		{
// 			name: "CancelPromiseMinimal",
// 			id:   "foo",
// 			serviceReqHeader: &CompletePromiseHeader{
// 				IdempotencyKey: nil,
// 				Strict:         false,
// 			},
// 			serviceReqBody: &CompletePromiseBody{
// 				Value: promise.Value{
// 					Headers: nil,
// 					Data:    nil,
// 				},
// 			},
// 			req: &t_api.Request{
// 				Kind: t_api.CancelPromise,
// 				CancelPromise: &t_api.CancelPromiseRequest{
// 					Id:             "foo",
// 					IdempotencyKey: nil,
// 					Strict:         false,
// 					Value: promise.Value{
// 						Headers: nil,
// 						Data:    nil,
// 					},
// 				},
// 			},
// 			res: &t_api.Response{
// 				Kind: t_api.CancelPromise,
// 				CancelPromise: &t_api.CompletePromiseResponse{
// 					Status: t_api.StatusCreated,
// 					Promise: &promise.Promise{
// 						Id:    "foo",
// 						State: promise.Canceled,
// 					},
// 				},
// 			},
// 		},
// 	} {
// 		t.Run(tc.name, func(t *testing.T) {
// 			serviceTest.Load(t, tc.req, tc.res)

// 			res, err := serviceTest.service.CancelPromise(tc.id, tc.serviceReqHeader, tc.serviceReqBody)
// 			if err != nil {
// 				t.Fatal(err)
// 			}

// 			assert.Equal(t, tc.res.CancelPromise, res)

// 		})
// 	}
// }

// func TestResolvePromise(t *testing.T) {
// 	serviceTest := setup()
// 	for _, tc := range []struct {
// 		name             string
// 		id               string
// 		serviceReqHeader *CompletePromiseHeader
// 		serviceReqBody   *CompletePromiseBody
// 		req              *t_api.Request
// 		res              *t_api.Response
// 	}{
// 		{
// 			name: "ResolvePromise",
// 			id:   "foo",
// 			serviceReqHeader: &CompletePromiseHeader{
// 				IdempotencyKey: test.IdempotencyKeyToPointer("bar"),
// 				Strict:         true,
// 			},
// 			serviceReqBody: &CompletePromiseBody{
// 				Value: promise.Value{
// 					Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
// 					Data:    []byte("cancel"),
// 				},
// 			},
// 			req: &t_api.Request{
// 				Kind: t_api.ResolvePromise,
// 				ResolvePromise: &t_api.ResolvePromiseRequest{
// 					Id:             "foo",
// 					IdempotencyKey: test.IdempotencyKeyToPointer("bar"),
// 					Strict:         true,
// 					Value: promise.Value{
// 						Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
// 						Data:    []byte("cancel"),
// 					},
// 				},
// 			},
// 			res: &t_api.Response{
// 				Kind: t_api.ResolvePromise,
// 				ResolvePromise: &t_api.CompletePromiseResponse{
// 					Status: t_api.StatusCreated,
// 					Promise: &promise.Promise{
// 						Id:    "foo",
// 						State: promise.Resolved,
// 					},
// 				},
// 			},
// 		},
// 		{
// 			name: "ResolvePromiseMinimal",
// 			id:   "foo",
// 			serviceReqHeader: &CompletePromiseHeader{
// 				IdempotencyKey: nil,
// 				Strict:         false,
// 			},
// 			serviceReqBody: &CompletePromiseBody{
// 				Value: promise.Value{
// 					Headers: nil,
// 					Data:    nil,
// 				},
// 			},
// 			req: &t_api.Request{
// 				Kind: t_api.ResolvePromise,
// 				ResolvePromise: &t_api.ResolvePromiseRequest{
// 					Id:             "foo",
// 					IdempotencyKey: nil,
// 					Strict:         false,
// 					Value: promise.Value{
// 						Headers: nil,
// 						Data:    nil,
// 					},
// 				},
// 			},
// 			res: &t_api.Response{
// 				Kind: t_api.ResolvePromise,
// 				ResolvePromise: &t_api.CompletePromiseResponse{
// 					Status: t_api.StatusCreated,
// 					Promise: &promise.Promise{
// 						Id:    "foo",
// 						State: promise.Resolved,
// 					},
// 				},
// 			},
// 		},
// 	} {
// 		t.Run(tc.name, func(t *testing.T) {
// 			serviceTest.Load(t, tc.req, tc.res)
// 			res, err := serviceTest.service.ResolvePromise(tc.id, tc.serviceReqHeader, tc.serviceReqBody)
// 			if err != nil {
// 				t.Fatal(err)
// 			}

// 			assert.Equal(t, tc.res.ResolvePromise, res)
// 		})
// 	}
// }

// func TestRejectPromise(t *testing.T) {
// 	serviceTest := setup()

// 	for _, tc := range []struct {
// 		name             string
// 		id               string
// 		serviceReqHeader *CompletePromiseHeader
// 		serviceReqBody   *CompletePromiseBody
// 		req              *t_api.Request
// 		res              *t_api.Response
// 	}{
// 		{
// 			name: "RejectPromise",
// 			id:   "foo",
// 			serviceReqHeader: &CompletePromiseHeader{
// 				IdempotencyKey: test.IdempotencyKeyToPointer("bar"),
// 				Strict:         true,
// 			},
// 			serviceReqBody: &CompletePromiseBody{
// 				Value: promise.Value{
// 					Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
// 					Data:    []byte("cancel"),
// 				},
// 			},
// 			req: &t_api.Request{
// 				Kind: t_api.RejectPromise,
// 				RejectPromise: &t_api.RejectPromiseRequest{
// 					Id:             "foo",
// 					IdempotencyKey: test.IdempotencyKeyToPointer("bar"),
// 					Strict:         true,
// 					Value: promise.Value{
// 						Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
// 						Data:    []byte("cancel"),
// 					},
// 				},
// 			},
// 			res: &t_api.Response{
// 				Kind: t_api.RejectPromise,
// 				RejectPromise: &t_api.CompletePromiseResponse{
// 					Status: t_api.StatusCreated,
// 					Promise: &promise.Promise{
// 						Id:    "foo",
// 						State: promise.Rejected,
// 					},
// 				},
// 			},
// 		},
// 		{
// 			name: "RejectPromiseMinimal",
// 			id:   "foo",
// 			serviceReqHeader: &CompletePromiseHeader{
// 				IdempotencyKey: nil,
// 				Strict:         false,
// 			},
// 			serviceReqBody: &CompletePromiseBody{
// 				Value: promise.Value{
// 					Headers: nil,
// 					Data:    nil,
// 				},
// 			},
// 			req: &t_api.Request{
// 				Kind: t_api.RejectPromise,
// 				RejectPromise: &t_api.RejectPromiseRequest{
// 					Id:             "foo",
// 					IdempotencyKey: nil,
// 					Strict:         false,
// 					Value: promise.Value{
// 						Headers: nil,
// 						Data:    nil,
// 					},
// 				},
// 			},
// 			res: &t_api.Response{
// 				Kind: t_api.RejectPromise,
// 				RejectPromise: &t_api.CompletePromiseResponse{
// 					Status: t_api.StatusCreated,
// 					Promise: &promise.Promise{
// 						Id:    "foo",
// 						State: promise.Rejected,
// 					},
// 				},
// 			},
// 		},
// 	} {
// 		t.Run(tc.name, func(t *testing.T) {
// 			serviceTest.Load(t, tc.req, tc.res)

// 			res, err := serviceTest.service.RejectPromise(tc.id, tc.serviceReqHeader, tc.serviceReqBody)
// 			if err != nil {
// 				t.Fatal(err)
// 			}

// 			assert.Equal(t, tc.res.RejectPromise, res)
// 		})
// 	}
// }

// // SCHEDULE

// func TestCreateSchedule(t *testing.T) {
// 	serviceTest := setup()
// 	tcs := []struct {
// 		name             string
// 		serviceReqHeader CreateScheduleHeader
// 		serviceReqBody   *CreateScheduleBody
// 		req              *t_api.Request
// 		res              *t_api.Response
// 	}{
// 		{
// 			name: "CreateScheduleMinimal",
// 			serviceReqHeader: CreateScheduleHeader{
// 				IdempotencyKey: test.IdempotencyKeyToPointer("bar"),
// 			},
// 			serviceReqBody: &CreateScheduleBody{
// 				Id:             "foo",
// 				Desc:           "",
// 				Cron:           "* * * * * *",
// 				PromiseId:      "foo.promise{{.timestamp}}",
// 				PromiseTimeout: 1000000,
// 			},
// 			req: &t_api.Request{
// 				Kind: t_api.CreateSchedule,
// 				CreateSchedule: &t_api.CreateScheduleRequest{
// 					Id:             "foo",
// 					IdempotencyKey: test.IdempotencyKeyToPointer("bar"),
// 					Cron:           "* * * * * *",
// 					PromiseId:      "foo.promise{{.timestamp}}",
// 					PromiseTimeout: 1000000,
// 				},
// 			},
// 			res: &t_api.Response{
// 				Kind: t_api.CreateSchedule,
// 				CreateSchedule: &t_api.CreateScheduleResponse{
// 					Status: t_api.StatusCreated,
// 					Schedule: &schedule.Schedule{
// 						Id:   "foo",
// 						Desc: "",
// 						Cron: "* * * * * *",
// 						// PromiseId:      "foo.promise{{.timestamp}}",
// 						PromiseTimeout: 1000000,
// 					},
// 				},
// 			},
// 		},
// 	}

// 	for _, tc := range tcs {
// 		t.Run(tc.name, func(t *testing.T) {
// 			serviceTest.Load(t, tc.req, tc.res)

// 			res, err := serviceTest.service.CreateSchedule(tc.serviceReqHeader, tc.serviceReqBody)
// 			if err != nil {
// 				t.Fatal(err)
// 			}

// 			assert.Equal(t, tc.res.CreateSchedule, res)
// 		})
// 	}
// }

// func TestReadSchedule(t *testing.T) {
// 	serviceTest := setup()
// 	tcs := []struct {
// 		name           string
// 		id             string
// 		req            *t_api.Request
// 		res            *t_api.Response
// 		expectedErrMsg *string
// 	}{
// 		{
// 			name: "ReadScheduleMinimal",
// 			id:   "foo",
// 			req: &t_api.Request{
// 				Kind: t_api.ReadSchedule,
// 				ReadSchedule: &t_api.ReadScheduleRequest{
// 					Id: "foo",
// 				},
// 			},
// 			res: &t_api.Response{
// 				Kind: t_api.ReadSchedule,
// 				ReadSchedule: &t_api.ReadScheduleResponse{
// 					Status: t_api.StatusCreated,
// 					Schedule: &schedule.Schedule{
// 						Id:             "foo",
// 						Desc:           "",
// 						Cron:           "* * * * * *",
// 						PromiseId:      "foo.promise.123123123",
// 						PromiseTimeout: 1000000,
// 					},
// 				},
// 			},
// 			expectedErrMsg: nil,
// 		},
// 	}

// 	for _, tc := range tcs {
// 		t.Run(tc.name, func(t *testing.T) {
// 			serviceTest.Load(t, tc.req, tc.res)

// 			res, err := serviceTest.service.ReadSchedule(tc.id)
// 			if err != nil {
// 				t.Fatal(err)
// 			}

// 			assert.Equal(t, tc.res.ReadSchedule, res)
// 		})
// 	}
// }

// func TestDeleteSchedule(t *testing.T) {
// 	serviceTest := setup()
// 	tcs := []struct {
// 		name string
// 		id   string
// 		req  *t_api.Request
// 		res  *t_api.Response
// 	}{
// 		{
// 			name: "DeleteScheduleMinimal",
// 			id:   "foo",
// 			req: &t_api.Request{
// 				Kind: t_api.DeleteSchedule,
// 				DeleteSchedule: &t_api.DeleteScheduleRequest{
// 					Id: "foo",
// 				},
// 			},
// 			res: &t_api.Response{
// 				Kind: t_api.DeleteSchedule,
// 				DeleteSchedule: &t_api.DeleteScheduleResponse{
// 					Status: t_api.StatusNoContent,
// 				},
// 			},
// 		},
// 	}

// 	for _, tc := range tcs {
// 		t.Run(tc.name, func(t *testing.T) {
// 			serviceTest.Load(t, tc.req, tc.res)

// 			res, err := serviceTest.service.DeleteSchedule(tc.id)
// 			if err != nil {
// 				t.Fatal(err)
// 			}

// 			assert.Equal(t, tc.res.DeleteSchedule, res)
// 		})
// 	}
// }

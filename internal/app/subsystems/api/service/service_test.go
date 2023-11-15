package service

import (
	"fmt"
	"testing"

	"github.com/resonatehq/resonate/internal/app/subsystems/api/test"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"

	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/stretchr/testify/assert"
)

type serviceTest struct {
	*test.API
	service *Service
}

func setup() *serviceTest {
	api := &test.API{}
	service := New(api, "local")

	return &serviceTest{
		API:     api,
		service: service,
	}
}

func TestReadPromise(t *testing.T) {
	serviceTest := setup()

	for _, tc := range []struct {
		name string
		id   string
		req  *t_api.Request
		res  *t_api.Response
	}{
		{
			name: "ReadPromise",
			id:   "foo",
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
		},
		{
			name: "ReadPromiseNotFound",
			id:   "bar",
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
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			serviceTest.Load(t, tc.req, tc.res)

			res, err := serviceTest.service.ReadPromise(tc.id, &Header{})
			if err != nil {
				t.Fatal(err)
			}

			assert.Equal(t, tc.res.ReadPromise, res)
		})
	}
}

func TestSearchPromises(t *testing.T) {
	serviceTest := setup()

	for _, tc := range []struct {
		name       string
		serviceReq *SearchPromiseParams
		req        *t_api.Request
		res        *t_api.Response
	}{
		{
			name: "SearchPromises",
			serviceReq: &SearchPromiseParams{
				Q:     "*",
				Limit: util.ToPointer(10),
			},
			req: &t_api.Request{
				Kind: t_api.SearchPromises,
				SearchPromises: &t_api.SearchPromisesRequest{
					Q: "*",
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
			serviceReq: &SearchPromiseParams{
				Cursor: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJOZXh0Ijp7InEiOiIqIiwic3RhdGVzIjpbIlBFTkRJTkciXSwibGltaXQiOjEwLCJzb3J0SWQiOjEwMH19.yQxXjIxRmxdTQcBDHFv8PyXxrkGa90e4OcIzDqPP1rY",
			},
			req: &t_api.Request{
				Kind: t_api.SearchPromises,
				SearchPromises: &t_api.SearchPromisesRequest{
					Q: "*",
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
					Status: t_api.StatusOK,
					Cursor: &t_api.Cursor[t_api.SearchPromisesRequest]{
						Next: &t_api.SearchPromisesRequest{
							Q: "*",
							States: []promise.State{
								promise.Pending,
								promise.Resolved,
								promise.Rejected,
								promise.Timedout,
								promise.Canceled,
							},
							Limit:  10,
							SortId: test.Int64ToPointer(10),
						},
					},
					Promises: []*promise.Promise{},
				},
			},
		},
		{
			name: "SearchPromisesPending",
			serviceReq: &SearchPromiseParams{
				Q:     "*",
				State: "pending",
				Limit: util.ToPointer(10),
			},
			req: &t_api.Request{
				Kind: t_api.SearchPromises,
				SearchPromises: &t_api.SearchPromisesRequest{
					Q: "*",
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
			serviceReq: &SearchPromiseParams{
				Q:     "*",
				State: "resolved",
				Limit: util.ToPointer(10),
			},
			req: &t_api.Request{
				Kind: t_api.SearchPromises,
				SearchPromises: &t_api.SearchPromisesRequest{
					Q: "*",
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
			serviceReq: &SearchPromiseParams{
				Q:     "*",
				State: "rejected",
				Limit: util.ToPointer(10),
			},
			req: &t_api.Request{
				Kind: t_api.SearchPromises,
				SearchPromises: &t_api.SearchPromisesRequest{
					Q: "*",
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
	} {
		t.Run(tc.name, func(t *testing.T) {
			serviceTest.Load(t, tc.req, tc.res)
			res, err := serviceTest.service.SearchPromises(&Header{}, tc.serviceReq)
			if err != nil {
				fmt.Println("we are here bro", res.Status)
				t.Fatal(err)
			}

			assert.Equal(t, tc.res.SearchPromises, res)
		})
	}
}

func TestCreatePromise(t *testing.T) {
	serviceTest := setup()

	for _, tc := range []struct {
		name             string
		id               string
		serviceReqHeader *CreatePromiseHeader
		serviceReqBody   *CreatePromiseBody
		req              *t_api.Request
		res              *t_api.Response
	}{
		{
			name: "CreatePromise",
			id:   "foo",
			serviceReqHeader: &CreatePromiseHeader{
				IdempotencyKey: test.IdempotencyKeyToPointer("bar"),
				Strict:         true,
			},
			serviceReqBody: &CreatePromiseBody{
				Param: &promise.Value{
					Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
					Data:    []byte("pending"),
				},
				Timeout: util.ToPointer(int64(1)),
			},

			req: &t_api.Request{
				Kind: t_api.CreatePromise,
				CreatePromise: &t_api.CreatePromiseRequest{
					Id:             "foo",
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
						Id:    "foo",
						State: promise.Pending,
					},
				},
			},
		},
		{
			name: "CreatePromiseMinimal",
			id:   "foo",
			serviceReqHeader: &CreatePromiseHeader{
				IdempotencyKey: nil,
				Strict:         false,
			},
			serviceReqBody: &CreatePromiseBody{
				Param: &promise.Value{
					Headers: nil,
					Data:    nil,
				},
				Timeout: util.ToPointer(int64(1)),
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
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			serviceTest.Load(t, tc.req, tc.res)
			res, err := serviceTest.service.CreatePromise(tc.id, tc.serviceReqHeader, tc.serviceReqBody)
			if err != nil {
				t.Fatal(err)
			}

			assert.Equal(t, tc.res.CreatePromise, res)
		})
	}
}

func TestCancelPromise(t *testing.T) {
	serviceTest := setup()

	for _, tc := range []struct {
		name             string
		id               string
		serviceReqHeader *CancelPromiseHeader
		serviceReqBody   *CancelPromiseBody
		req              *t_api.Request
		res              *t_api.Response
	}{
		{
			name: "CancelPromise",
			id:   "foo",
			serviceReqHeader: &CancelPromiseHeader{
				IdempotencyKey: test.IdempotencyKeyToPointer("bar"),
				Strict:         true,
			},
			serviceReqBody: &CancelPromiseBody{
				Value: promise.Value{
					Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
					Data:    []byte("cancel"),
				},
			},
			req: &t_api.Request{
				Kind: t_api.CancelPromise,
				CancelPromise: &t_api.CancelPromiseRequest{
					Id:             "foo",
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
				CancelPromise: &t_api.CancelPromiseResponse{
					Status: t_api.StatusCreated,
					Promise: &promise.Promise{
						Id:    "foo",
						State: promise.Canceled,
					},
				},
			},
		},
		{
			name: "CancelPromiseMinimal",
			id:   "foo",
			serviceReqHeader: &CancelPromiseHeader{
				IdempotencyKey: nil,
				Strict:         false,
			},
			serviceReqBody: &CancelPromiseBody{
				Value: promise.Value{
					Headers: nil,
					Data:    nil,
				},
			},
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
				CancelPromise: &t_api.CancelPromiseResponse{
					Status: t_api.StatusCreated,
					Promise: &promise.Promise{
						Id:    "foo",
						State: promise.Canceled,
					},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			serviceTest.Load(t, tc.req, tc.res)

			res, err := serviceTest.service.CancelPromise(tc.id, tc.serviceReqHeader, tc.serviceReqBody)
			if err != nil {
				t.Fatal(err)
			}

			assert.Equal(t, tc.res.CancelPromise, res)

		})
	}
}

func TestResolvePromise(t *testing.T) {
	serviceTest := setup()
	for _, tc := range []struct {
		name             string
		id               string
		serviceReqHeader *ResolvePromiseHeader
		serviceReqBody   *ResolvePromiseBody
		req              *t_api.Request
		res              *t_api.Response
	}{
		{
			name: "ResolvePromise",
			id:   "foo",
			serviceReqHeader: &ResolvePromiseHeader{
				IdempotencyKey: test.IdempotencyKeyToPointer("bar"),
				Strict:         true,
			},
			serviceReqBody: &ResolvePromiseBody{
				Value: promise.Value{
					Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
					Data:    []byte("cancel"),
				},
			},
			req: &t_api.Request{
				Kind: t_api.ResolvePromise,
				ResolvePromise: &t_api.ResolvePromiseRequest{
					Id:             "foo",
					IdempotencyKey: test.IdempotencyKeyToPointer("bar"),
					Strict:         true,
					Value: promise.Value{
						Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
						Data:    []byte("cancel"),
					},
				},
			},
			res: &t_api.Response{
				Kind: t_api.ResolvePromise,
				ResolvePromise: &t_api.ResolvePromiseResponse{
					Status: t_api.StatusCreated,
					Promise: &promise.Promise{
						Id:    "foo",
						State: promise.Resolved,
					},
				},
			},
		},
		{
			name: "ResolvePromiseMinimal",
			id:   "foo",
			serviceReqHeader: &ResolvePromiseHeader{
				IdempotencyKey: nil,
				Strict:         false,
			},
			serviceReqBody: &ResolvePromiseBody{
				Value: promise.Value{
					Headers: nil,
					Data:    nil,
				},
			},
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
				ResolvePromise: &t_api.ResolvePromiseResponse{
					Status: t_api.StatusCreated,
					Promise: &promise.Promise{
						Id:    "foo",
						State: promise.Resolved,
					},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			serviceTest.Load(t, tc.req, tc.res)
			res, err := serviceTest.service.ResolvePromise(tc.id, tc.serviceReqHeader, tc.serviceReqBody)
			if err != nil {
				t.Fatal(err)
			}

			assert.Equal(t, tc.res.ResolvePromise, res)
		})
	}
}

func TestRejectPromise(t *testing.T) {
	serviceTest := setup()

	for _, tc := range []struct {
		name             string
		id               string
		serviceReqHeader *RejectPromiseHeader
		serviceReqBody   *RejectPromiseBody
		req              *t_api.Request
		res              *t_api.Response
	}{
		{
			name: "RejectPromise",
			id:   "foo",
			serviceReqHeader: &RejectPromiseHeader{
				IdempotencyKey: test.IdempotencyKeyToPointer("bar"),
				Strict:         true,
			},
			serviceReqBody: &RejectPromiseBody{
				Value: promise.Value{
					Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
					Data:    []byte("cancel"),
				},
			},
			req: &t_api.Request{
				Kind: t_api.RejectPromise,
				RejectPromise: &t_api.RejectPromiseRequest{
					Id:             "foo",
					IdempotencyKey: test.IdempotencyKeyToPointer("bar"),
					Strict:         true,
					Value: promise.Value{
						Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
						Data:    []byte("cancel"),
					},
				},
			},
			res: &t_api.Response{
				Kind: t_api.RejectPromise,
				RejectPromise: &t_api.RejectPromiseResponse{
					Status: t_api.StatusCreated,
					Promise: &promise.Promise{
						Id:    "foo",
						State: promise.Rejected,
					},
				},
			},
		},
		{
			name: "RejectPromiseMinimal",
			id:   "foo",
			serviceReqHeader: &RejectPromiseHeader{
				IdempotencyKey: nil,
				Strict:         false,
			},
			serviceReqBody: &RejectPromiseBody{
				Value: promise.Value{
					Headers: nil,
					Data:    nil,
				},
			},
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
				Kind: t_api.RejectPromise,
				RejectPromise: &t_api.RejectPromiseResponse{
					Status: t_api.StatusCreated,
					Promise: &promise.Promise{
						Id:    "foo",
						State: promise.Rejected,
					},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			serviceTest.Load(t, tc.req, tc.res)

			res, err := serviceTest.service.RejectPromise(tc.id, tc.serviceReqHeader, tc.serviceReqBody)
			if err != nil {
				t.Fatal(err)
			}

			assert.Equal(t, tc.res.RejectPromise, res)
		})
	}
}

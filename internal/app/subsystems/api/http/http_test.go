package http

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/stretchr/testify/assert"
)

type testAPI struct {
	t   *testing.T
	req *types.Request
	res *types.Response
}

func (a *testAPI) String() string {
	return "api:test"
}

func (a *testAPI) Enqueue(sqe *bus.SQE[types.Request, types.Response]) {
	// assert
	assert.Equal(a.t, a.req, sqe.Submission)

	// immediately call callback
	go sqe.Callback(0, a.res, nil)
}

func (a *testAPI) Dequeue(int, <-chan time.Time) []*bus.SQE[types.Request, types.Response] {
	return nil
}

func (a *testAPI) Done() bool {
	return false
}

func TestHttpServer(t *testing.T) {
	api := &testAPI{}

	client := &http.Client{
		Timeout: 1 * time.Second,
	}

	server := New(api, &Config{
		Addr:    "127.0.0.1:8888",
		Timeout: 0,
	})

	// start the server
	errors := make(chan error)
	defer close(errors)

	go server.Start(errors)
	time.Sleep(100 * time.Millisecond)

	for _, tc := range []struct {
		name    string
		path    string
		method  string
		headers map[string]string
		body    []byte
		req     *types.Request
		res     *types.Response
		status  int
	}{
		{
			name:   "ReadPromise",
			path:   "promises/foo",
			method: "GET",
			req: &types.Request{
				Kind: types.ReadPromise,
				ReadPromise: &types.ReadPromiseRequest{
					Id: "foo",
				},
			},
			res: &types.Response{
				Kind: types.ReadPromise,
				ReadPromise: &types.ReadPromiseResponse{
					Status:  types.ResponseOK,
					Promise: nil,
				},
			},
			status: 200,
		},
		{
			name:   "ReadPromiseNotFound",
			path:   "promises/bar",
			method: "GET",
			req: &types.Request{
				Kind: types.ReadPromise,
				ReadPromise: &types.ReadPromiseRequest{
					Id: "bar",
				},
			},
			res: &types.Response{
				Kind: types.ReadPromise,
				ReadPromise: &types.ReadPromiseResponse{
					Status:  types.ResponseNotFound,
					Promise: nil,
				},
			},
			status: 404,
		},
		{
			name:   "SearchPromises",
			path:   "promises?q=*&limit=10",
			method: "GET",
			req: &types.Request{
				Kind: types.SearchPromises,
				SearchPromises: &types.SearchPromisesRequest{
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
			res: &types.Response{
				Kind: types.SearchPromises,
				SearchPromises: &types.SearchPromisesResponse{
					Status:   types.ResponseOK,
					Cursor:   nil,
					Promises: nil,
				},
			},
			status: 200,
		},
		{
			name:   "SearchPromisesCursor",
			path:   "promises?cursor=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJOZXh0Ijp7InEiOiIqIiwic3RhdGVzIjpbIlBFTkRJTkciXSwibGltaXQiOjEwLCJzb3J0SWQiOjEwMH19.yQxXjIxRmxdTQcBDHFv8PyXxrkGa90e4OcIzDqPP1rY",
			method: "GET",
			req: &types.Request{
				Kind: types.SearchPromises,
				SearchPromises: &types.SearchPromisesRequest{
					Q: "*",
					States: []promise.State{
						promise.Pending,
					},
					Limit:  10,
					SortId: int64ToPointer(100),
				},
			},
			res: &types.Response{
				Kind: types.SearchPromises,
				SearchPromises: &types.SearchPromisesResponse{
					Status:   types.ResponseOK,
					Cursor:   nil,
					Promises: nil,
				},
			},
			status: 200,
		},
		{
			name:   "SearchPromisesPending",
			path:   "promises?q=*&state=pending",
			method: "GET",
			req: &types.Request{
				Kind: types.SearchPromises,
				SearchPromises: &types.SearchPromisesRequest{
					Q: "*",
					States: []promise.State{
						promise.Pending,
					},
				},
			},
			res: &types.Response{
				Kind: types.SearchPromises,
				SearchPromises: &types.SearchPromisesResponse{
					Status:   types.ResponseOK,
					Cursor:   nil,
					Promises: nil,
				},
			},
			status: 200,
		},
		{
			name:   "SearchPromisesResolved",
			path:   "promises?q=*&state=resolved",
			method: "GET",
			req: &types.Request{
				Kind: types.SearchPromises,
				SearchPromises: &types.SearchPromisesRequest{
					Q: "*",
					States: []promise.State{
						promise.Resolved,
					},
				},
			},
			res: &types.Response{
				Kind: types.SearchPromises,
				SearchPromises: &types.SearchPromisesResponse{
					Status:   types.ResponseOK,
					Cursor:   nil,
					Promises: nil,
				},
			},
			status: 200,
		},
		{
			name:   "SearchPromisesRejected",
			path:   "promises?q=*&state=rejected",
			method: "GET",
			req: &types.Request{
				Kind: types.SearchPromises,
				SearchPromises: &types.SearchPromisesRequest{
					Q: "*",
					States: []promise.State{
						promise.Rejected,
						promise.Timedout,
						promise.Canceled,
					},
				},
			},
			res: &types.Response{
				Kind: types.SearchPromises,
				SearchPromises: &types.SearchPromisesResponse{
					Status:   types.ResponseOK,
					Cursor:   nil,
					Promises: nil,
				},
			},
			status: 200,
		},
		{
			name:   "CreatePromise",
			path:   "promises/foo/create",
			method: "POST",
			headers: map[string]string{
				"Idempotency-Key": "bar",
				"Strict":          "true",
			},
			body: []byte(`{
				"param": {
					"headers": {"a":"a","b":"b","c":"c"},
					"data": "cGVuZGluZw=="
				},
				"timeout": 1
			}`),
			req: &types.Request{
				Kind: types.CreatePromise,
				CreatePromise: &types.CreatePromiseRequest{
					Id:             "foo",
					IdempotencyKey: idempotencyKeyToPointer("bar"),
					Strict:         true,
					Param: promise.Value{
						Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
						Data:    []byte("pending"),
					},
					Timeout: 1,
				},
			},
			res: &types.Response{
				Kind: types.CreatePromise,
				CreatePromise: &types.CreatePromiseResponse{
					Status:  types.ResponseCreated,
					Promise: nil,
				},
			},
			status: 201,
		},
		{
			name:   "CreatePromiseMinimal",
			path:   "promises/foo/create",
			method: "POST",
			body: []byte(`{
				"timeout": 1
			}`),
			req: &types.Request{
				Kind: types.CreatePromise,
				CreatePromise: &types.CreatePromiseRequest{
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
			res: &types.Response{
				Kind: types.CreatePromise,
				CreatePromise: &types.CreatePromiseResponse{
					Status:  types.ResponseCreated,
					Promise: nil,
				},
			},
			status: 201,
		},
		{
			name:   "CancelPromise",
			path:   "promises/foo/cancel",
			method: "POST",
			headers: map[string]string{
				"Idempotency-Key": "bar",
				"Strict":          "true",
			},
			body: []byte(`{
				"value": {
					"headers": {"a":"a","b":"b","c":"c"},
					"data": "Y2FuY2Vs"
				}
			}`),
			req: &types.Request{
				Kind: types.CancelPromise,
				CancelPromise: &types.CancelPromiseRequest{
					Id:             "foo",
					IdempotencyKey: idempotencyKeyToPointer("bar"),
					Strict:         true,
					Value: promise.Value{
						Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
						Data:    []byte("cancel"),
					},
				},
			},
			res: &types.Response{
				Kind: types.CancelPromise,
				CancelPromise: &types.CancelPromiseResponse{
					Status:  types.ResponseCreated,
					Promise: nil,
				},
			},
			status: 201,
		},
		{
			name:   "CancelPromiseMinimal",
			path:   "promises/foo/cancel",
			method: "POST",
			body:   []byte(`{}`),
			req: &types.Request{
				Kind: types.CancelPromise,
				CancelPromise: &types.CancelPromiseRequest{
					Id:             "foo",
					IdempotencyKey: nil,
					Strict:         false,
					Value: promise.Value{
						Headers: nil,
						Data:    nil,
					},
				},
			},
			res: &types.Response{
				Kind: types.CancelPromise,
				CancelPromise: &types.CancelPromiseResponse{
					Status:  types.ResponseCreated,
					Promise: nil,
				},
			},
			status: 201,
		},
		{
			name:   "ResolvePromise",
			path:   "promises/foo/resolve",
			method: "POST",
			headers: map[string]string{
				"Idempotency-Key": "bar",
				"Strict":          "true",
			},
			body: []byte(`{
				"value": {
					"headers": {"a":"a","b":"b","c":"c"},
					"data": "cmVzb2x2ZQ=="
				}
			}`),
			req: &types.Request{
				Kind: types.ResolvePromise,
				ResolvePromise: &types.ResolvePromiseRequest{
					Id:             "foo",
					IdempotencyKey: idempotencyKeyToPointer("bar"),
					Strict:         true,
					Value: promise.Value{
						Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
						Data:    []byte("resolve"),
					},
				},
			},
			res: &types.Response{
				Kind: types.ResolvePromise,
				ResolvePromise: &types.ResolvePromiseResponse{
					Status:  types.ResponseCreated,
					Promise: nil,
				},
			},
			status: 201,
		},
		{
			name:   "ResolvePromiseMinimal",
			path:   "promises/foo/resolve",
			method: "POST",
			body:   []byte(`{}`),
			req: &types.Request{
				Kind: types.ResolvePromise,
				ResolvePromise: &types.ResolvePromiseRequest{
					Id:             "foo",
					IdempotencyKey: nil,
					Strict:         false,
					Value: promise.Value{
						Headers: nil,
						Data:    nil,
					},
				},
			},
			res: &types.Response{
				Kind: types.ResolvePromise,
				ResolvePromise: &types.ResolvePromiseResponse{
					Status:  types.ResponseCreated,
					Promise: nil,
				},
			},
			status: 201,
		},
		{
			name:   "RejectPromise",
			path:   "promises/foo/reject",
			method: "POST",
			headers: map[string]string{
				"Idempotency-Key": "bar",
				"Strict":          "true",
			},
			body: []byte(`{
				"value": {
					"headers": {"a":"a","b":"b","c":"c"},
					"data": "cmVqZWN0"
				}
			}`),
			req: &types.Request{
				Kind: types.RejectPromise,
				RejectPromise: &types.RejectPromiseRequest{
					Id:             "foo",
					IdempotencyKey: idempotencyKeyToPointer("bar"),
					Strict:         true,
					Value: promise.Value{
						Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
						Data:    []byte("reject"),
					},
				},
			},
			res: &types.Response{
				Kind: types.RejectPromise,
				RejectPromise: &types.RejectPromiseResponse{
					Status:  types.ResponseCreated,
					Promise: nil,
				},
			},
			status: 201,
		},
		{
			name:   "RejectPromiseMinimal",
			path:   "promises/foo/reject",
			method: "POST",
			body:   []byte(`{}`),
			req: &types.Request{
				Kind: types.RejectPromise,
				RejectPromise: &types.RejectPromiseRequest{
					Id:             "foo",
					IdempotencyKey: nil,
					Strict:         false,
					Value: promise.Value{
						Headers: nil,
						Data:    nil,
					},
				},
			},
			res: &types.Response{
				Kind: types.ResolvePromise,
				RejectPromise: &types.RejectPromiseResponse{
					Status:  types.ResponseCreated,
					Promise: nil,
				},
			},
			status: 201,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api.t = t
			api.req = tc.req
			api.res = tc.res

			req, err := http.NewRequest(tc.method, fmt.Sprintf("http://127.0.0.1:8888/%s", tc.path), bytes.NewBuffer(tc.body))
			if err != nil {
				t.Fatal(err)
			}

			// set headers
			req.Header.Set("Content-Type", "application/json")
			for key, val := range tc.headers {
				req.Header.Set(key, val)
			}

			res, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()

			body, err := io.ReadAll(res.Body)
			if err != nil {
				t.Fatal(err)
			}

			assert.Equal(t, tc.status, res.StatusCode, string(body))

			select {
			case err := <-errors:
				t.Fatal(err)
			default:
			}
		})
	}

	// stop the server
	if err := server.Stop(); err != nil {
		t.Fatal(err)
	}
}

func idempotencyKeyToPointer(s string) *promise.IdempotencyKey {
	idempotencyKey := promise.IdempotencyKey(s)
	return &idempotencyKey
}

func int64ToPointer(i int64) *int64 {
	return &i
}

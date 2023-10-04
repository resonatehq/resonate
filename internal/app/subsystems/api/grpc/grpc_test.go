package grpc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/resonatehq/resonate/internal/api"
	grpcApi "github.com/resonatehq/resonate/internal/app/subsystems/api/grpc/api"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/test"
	"github.com/resonatehq/resonate/pkg/promise"

	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type grpcTest struct {
	*test.API
	subsystem api.Subsystem
	errors    chan error
	conn      *grpc.ClientConn
	client    grpcApi.PromiseServiceClient
}

func setup() (*grpcTest, error) {
	api := &test.API{}
	errors := make(chan error)
	subsystem := New(api, &Config{
		Addr: "127.0.0.1:5555",
	})

	// start grpc server
	go subsystem.Start(errors)
	time.Sleep(100 * time.Millisecond)

	conn, err := grpc.Dial("127.0.0.1:5555", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return &grpcTest{
		API:       api,
		subsystem: subsystem,
		errors:    errors,
		conn:      conn,
		client:    grpcApi.NewPromiseServiceClient(conn),
	}, nil
}

func (t *grpcTest) teardown() error {
	defer close(t.errors)

	if err := t.conn.Close(); err != nil {
		return err
	}

	return t.subsystem.Stop()
}

func TestReadPromise(t *testing.T) {
	grpcTest, err := setup()
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name    string
		grpcReq *grpcApi.ReadPromiseRequest
		req     *t_api.Request
		res     *t_api.Response
		status  grpcApi.Status
	}{
		{
			name: "ReadPromise",
			grpcReq: &grpcApi.ReadPromiseRequest{
				Id: "foo",
			},
			req: &t_api.Request{
				Kind: t_api.ReadPromise,
				ReadPromise: &t_api.ReadPromiseRequest{
					Id: "foo",
				},
			},
			res: &t_api.Response{
				Kind: t_api.ReadPromise,
				ReadPromise: &t_api.ReadPromiseResponse{
					Status:  t_api.ResponseOK,
					Promise: nil,
				},
			},
			status: 200,
		},
		{
			name: "ReadPromiseNotFound",
			grpcReq: &grpcApi.ReadPromiseRequest{
				Id: "bar",
			},
			req: &t_api.Request{
				Kind: t_api.ReadPromise,
				ReadPromise: &t_api.ReadPromiseRequest{
					Id: "bar",
				},
			},
			res: &t_api.Response{
				Kind: t_api.ReadPromise,
				ReadPromise: &t_api.ReadPromiseResponse{
					Status:  t_api.ResponseNotFound,
					Promise: nil,
				},
			},
			status: 404,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			grpcTest.Load(t, tc.req, tc.res)

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			res, err := grpcTest.client.ReadPromise(ctx, tc.grpcReq)
			if err != nil {
				t.Fatal(err)
			}

			assert.Equal(t, tc.status, res.Status)

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

func TestSearchPromises(t *testing.T) {
	grpcTest, err := setup()
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name    string
		grpcReq *grpcApi.SearchPromisesRequest
		req     *t_api.Request
		res     *t_api.Response
		status  grpcApi.Status
	}{
		{
			name: "SearchPromises",
			grpcReq: &grpcApi.SearchPromisesRequest{
				Q:     "*",
				Limit: 10,
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
					Status:   t_api.ResponseOK,
					Cursor:   nil,
					Promises: nil,
				},
			},
			status: 200,
		},
		{
			name: "SearchPromisesCursor",
			grpcReq: &grpcApi.SearchPromisesRequest{
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
					Status:   t_api.ResponseOK,
					Cursor:   nil,
					Promises: nil,
				},
			},
			status: 200,
		},
		{
			name: "SearchPromisesPending",
			grpcReq: &grpcApi.SearchPromisesRequest{
				Q:     "*",
				State: grpcApi.SearchState_SEARCH_PENDING,
			},
			req: &t_api.Request{
				Kind: t_api.SearchPromises,
				SearchPromises: &t_api.SearchPromisesRequest{
					Q: "*",
					States: []promise.State{
						promise.Pending,
					},
				},
			},
			res: &t_api.Response{
				Kind: t_api.SearchPromises,
				SearchPromises: &t_api.SearchPromisesResponse{
					Status:   t_api.ResponseOK,
					Cursor:   nil,
					Promises: nil,
				},
			},
			status: 200,
		},
		{
			name: "SearchPromisesResolved",
			grpcReq: &grpcApi.SearchPromisesRequest{
				Q:     "*",
				State: grpcApi.SearchState_SEARCH_RESOLVED,
			},
			req: &t_api.Request{
				Kind: t_api.SearchPromises,
				SearchPromises: &t_api.SearchPromisesRequest{
					Q: "*",
					States: []promise.State{
						promise.Resolved,
					},
				},
			},
			res: &t_api.Response{
				Kind: t_api.SearchPromises,
				SearchPromises: &t_api.SearchPromisesResponse{
					Status:   t_api.ResponseOK,
					Cursor:   nil,
					Promises: nil,
				},
			},
			status: 200,
		},
		{
			name: "SearchPromisesRejected",
			grpcReq: &grpcApi.SearchPromisesRequest{
				Q:     "*",
				State: grpcApi.SearchState_SEARCH_REJECTED,
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
				},
			},
			res: &t_api.Response{
				Kind: t_api.SearchPromises,
				SearchPromises: &t_api.SearchPromisesResponse{
					Status:   t_api.ResponseOK,
					Cursor:   nil,
					Promises: nil,
				},
			},
			status: 200,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			grpcTest.Load(t, tc.req, tc.res)

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			res, err := grpcTest.client.SearchPromises(ctx, tc.grpcReq)
			if err != nil {
				fmt.Println("we are here bro", res.Status)
				t.Fatal(err)
			}

			assert.Equal(t, tc.status, res.Status)

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

func TestCreatePromise(t *testing.T) {
	grpcTest, err := setup()
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name    string
		grpcReq *grpcApi.CreatePromiseRequest
		req     *t_api.Request
		res     *t_api.Response
		status  grpcApi.Status
	}{
		{
			name: "CreatePromise",
			grpcReq: &grpcApi.CreatePromiseRequest{
				Id:             "foo",
				IdempotencyKey: "bar",
				Strict:         true,
				Param: &grpcApi.Value{
					Headers: map[string]string{"a": "a", "b": "b", "c": "c"},
					Data:    []byte("pending"),
				},
				Timeout: 1,
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
					Status:  t_api.ResponseCreated,
					Promise: nil,
				},
			},
			status: 201,
		},
		{
			name: "CreatePromiseMinimal",
			grpcReq: &grpcApi.CreatePromiseRequest{
				Id:      "foo",
				Timeout: 1,
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
					Status:  t_api.ResponseCreated,
					Promise: nil,
				},
			},
			status: 201,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			grpcTest.Load(t, tc.req, tc.res)

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			res, err := grpcTest.client.CreatePromise(ctx, tc.grpcReq)
			if err != nil {
				t.Fatal(err)
			}

			assert.Equal(t, tc.status, res.Status)

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

func TestCancelPromise(t *testing.T) {
	grpcTest, err := setup()
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name    string
		grpcReq *grpcApi.CancelPromiseRequest
		req     *t_api.Request
		res     *t_api.Response
		status  grpcApi.Status
	}{
		{
			name: "CancelPromise",
			grpcReq: &grpcApi.CancelPromiseRequest{
				Id:             "foo",
				IdempotencyKey: "bar",
				Strict:         true,
				Value: &grpcApi.Value{
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
					Status:  t_api.ResponseCreated,
					Promise: nil,
				},
			},
			status: 201,
		},
		{
			name: "CancelPromiseMinimal",
			grpcReq: &grpcApi.CancelPromiseRequest{
				Id: "foo",
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
					Status:  t_api.ResponseCreated,
					Promise: nil,
				},
			},
			status: 201,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			grpcTest.Load(t, tc.req, tc.res)

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			res, err := grpcTest.client.CancelPromise(ctx, tc.grpcReq)
			if err != nil {
				t.Fatal(err)
			}

			assert.Equal(t, tc.status, res.Status)

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

func TestResolvePromise(t *testing.T) {
	grpcTest, err := setup()
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name    string
		grpcReq *grpcApi.ResolvePromiseRequest
		req     *t_api.Request
		res     *t_api.Response
		status  grpcApi.Status
	}{
		{
			name: "ResolvePromise",
			grpcReq: &grpcApi.ResolvePromiseRequest{
				Id:             "foo",
				IdempotencyKey: "bar",
				Strict:         true,
				Value: &grpcApi.Value{
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
					Status:  t_api.ResponseCreated,
					Promise: nil,
				},
			},
			status: 201,
		},
		{
			name: "ResolvePromiseMinimal",
			grpcReq: &grpcApi.ResolvePromiseRequest{
				Id: "foo",
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
					Status:  t_api.ResponseCreated,
					Promise: nil,
				},
			},
			status: 201,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			grpcTest.Load(t, tc.req, tc.res)

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			res, err := grpcTest.client.ResolvePromise(ctx, tc.grpcReq)
			if err != nil {
				t.Fatal(err)
			}

			assert.Equal(t, tc.status, res.Status)

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

func TestRejectPromise(t *testing.T) {
	grpcTest, err := setup()
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name    string
		grpcReq *grpcApi.RejectPromiseRequest
		req     *t_api.Request
		res     *t_api.Response
		status  grpcApi.Status
	}{
		{
			name: "RejectPromise",
			grpcReq: &grpcApi.RejectPromiseRequest{
				Id:             "foo",
				IdempotencyKey: "bar",
				Strict:         true,
				Value: &grpcApi.Value{
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
					Status:  t_api.ResponseCreated,
					Promise: nil,
				},
			},
			status: 201,
		},
		{
			name: "RejectPromiseMinimal",
			grpcReq: &grpcApi.RejectPromiseRequest{
				Id: "foo",
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
					Status:  t_api.ResponseCreated,
					Promise: nil,
				},
			},
			status: 201,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			grpcTest.Load(t, tc.req, tc.res)

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			res, err := grpcTest.client.RejectPromise(ctx, tc.grpcReq)
			if err != nil {
				t.Fatal(err)
			}

			assert.Equal(t, tc.status, res.Status)

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

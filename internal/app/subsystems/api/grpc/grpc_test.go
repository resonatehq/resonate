package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/resonatehq/resonate/internal/api"
	grpcApi "github.com/resonatehq/resonate/internal/app/subsystems/api/grpc/api"

	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

func setup(t *testing.T) (*testAPI, api.Subsystem, *grpc.ClientConn, grpcApi.PromiseServiceClient) {
	api := &testAPI{}

	server := New(api, &Config{
		Addr: "127.0.0.1:5555",
	})

	// start the server
	errors := make(chan error)
	defer close(errors)

	go server.Start(errors)
	time.Sleep(100 * time.Millisecond)

	conn, err := grpc.Dial("127.0.0.1:5555", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}

	return api, server, conn, grpcApi.NewPromiseServiceClient(conn)
}

func teardown(t *testing.T, server api.Subsystem, conn *grpc.ClientConn) {
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := server.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestReadPromise(t *testing.T) {
	api, server, conn, client := setup(t)
	defer teardown(t, server, conn)

	for _, tc := range []struct {
		name    string
		grpcReq *grpcApi.ReadPromiseRequest
		req     *types.Request
		res     *types.Response
		status  grpcApi.Status
	}{
		{
			name: "ReadPromise",
			grpcReq: &grpcApi.ReadPromiseRequest{
				Id: "foo",
			},
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
			name: "ReadPromiseNotFound",
			grpcReq: &grpcApi.ReadPromiseRequest{
				Id: "bar",
			},
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
	} {
		t.Run(tc.name, func(t *testing.T) {
			api.t = t
			api.req = tc.req
			api.res = tc.res

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			res, err := client.ReadPromise(ctx, tc.grpcReq)
			if err != nil {
				t.Fatal(err)
			}

			assert.Equal(t, tc.status, res.Status)
		})
	}
}

// func idempotencyKeyToPointer(s string) *promise.IdempotencyKey {
// 	idempotencyKey := promise.IdempotencyKey(s)
// 	return &idempotencyKey
// }

// func int64ToPointer(i int64) *int64 {
// 	return &i
// }

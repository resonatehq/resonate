// update proto
package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/grpc/pb"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/test"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type grpcTest struct {
	*test.API
	subsystem api.Subsystem
	errors    chan error
	conn      *grpc.ClientConn
	promises  pb.PromisesClient
	callbacks pb.CallbacksClient
	schedules pb.SchedulesClient
	locks     pb.LocksClient
	tasks     pb.TasksClient
}

func setup() (*grpcTest, error) {
	api := &test.API{}
	errors := make(chan error)
	subsystem, err := New(api, &Config{Addr: ":0"})
	if err != nil {
		return nil, err
	}

	// start grpc server
	go subsystem.Start(errors)
	time.Sleep(100 * time.Millisecond)

	conn, err := grpc.NewClient(subsystem.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return &grpcTest{
		API:       api,
		subsystem: subsystem,
		errors:    errors,
		conn:      conn,
		promises:  pb.NewPromisesClient(conn),
		callbacks: pb.NewCallbacksClient(conn),
		schedules: pb.NewSchedulesClient(conn),
		locks:     pb.NewLocksClient(conn),
		tasks:     pb.NewTasksClient(conn),
	}, nil
}

func (t *grpcTest) teardown() error {
	defer close(t.errors)

	if err := t.conn.Close(); err != nil {
		return err
	}

	return t.subsystem.Stop()
}

func TestGrpc(t *testing.T) {
	grpcTest, err := setup()
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range test.TestCases {
		t.Run(tc.Name, func(t *testing.T) {
			if tc.Req != nil {
				// set protocol specific header
				tc.Req.Tags["protocol"] = "grpc"
			}

			grpcTest.Load(t, tc.Req, tc.Res)

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			var res protoreflect.ProtoMessage
			var err error

			switch req := tc.Grpc.Req.(type) {
			case *pb.ReadPromiseRequest:
				_, err = grpcTest.promises.ReadPromise(ctx, req)
			case *pb.SearchPromisesRequest:
				_, err = grpcTest.promises.SearchPromises(ctx, req)
			case *pb.CreatePromiseRequest:
				res, err = grpcTest.promises.CreatePromise(ctx, req)
			case *pb.CreatePromiseAndTaskRequest:
				res, err = grpcTest.promises.CreatePromiseAndTask(ctx, req)
			case *pb.CreatePromiseAndCallbackRequest:
				res, err = grpcTest.promises.CreatePromiseAndCallback(ctx, req)
			case *pb.ResolvePromiseRequest:
				res, err = grpcTest.promises.ResolvePromise(ctx, req)
			case *pb.RejectPromiseRequest:
				res, err = grpcTest.promises.RejectPromise(ctx, req)
			case *pb.CancelPromiseRequest:
				res, err = grpcTest.promises.CancelPromise(ctx, req)
			case *pb.CreateCallbackRequest:
				_, err = grpcTest.callbacks.CreateCallback(ctx, req)
			case *pb.ReadScheduleRequest:
				_, err = grpcTest.schedules.ReadSchedule(ctx, req)
			case *pb.SearchSchedulesRequest:
				_, err = grpcTest.schedules.SearchSchedules(ctx, req)
			case *pb.CreateScheduleRequest:
				res, err = grpcTest.schedules.CreateSchedule(ctx, req)
			case *pb.DeleteScheduleRequest:
				_, err = grpcTest.schedules.DeleteSchedule(ctx, req)
			case *pb.AcquireLockRequest:
				_, err = grpcTest.locks.AcquireLock(ctx, req)
			case *pb.ReleaseLockRequest:
				_, err = grpcTest.locks.ReleaseLock(ctx, req)
			case *pb.HeartbeatLocksRequest:
				_, err = grpcTest.locks.HeartbeatLocks(ctx, req)
			case *pb.ClaimTaskRequest:
				res, err = grpcTest.tasks.ClaimTask(ctx, req)
			case *pb.CompleteTaskRequest:
				_, err = grpcTest.tasks.CompleteTask(ctx, req)
			case *pb.HeartbeatTasksRequest:
				_, err = grpcTest.tasks.HeartbeatTasks(ctx, req)
			default:
				t.Fatalf("unexpected type %T", req)
			}

			// assert successful response
			if tc.Grpc.Res != nil {
				assert.True(t, proto.Equal(tc.Grpc.Res, res), "%v\n%v", tc.Grpc.Res, res)
			}

			// assert error response
			if err != nil {
				// actual grpc error
				s, ok := status.FromError(err)
				if !ok {
					t.Fatal(err)
				}

				assert.Equal(t, tc.Grpc.Code, s.Code(), "%s != %s", tc.Grpc.Code, s.Code())
			}

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

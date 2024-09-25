package grpc

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"github.com/resonatehq/resonate/internal/app/subsystems/api/service"
	"github.com/resonatehq/resonate/internal/kernel/t_api"

	"github.com/resonatehq/resonate/internal/api"
	grpcApi "github.com/resonatehq/resonate/internal/app/subsystems/api/grpc/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type Config struct {
	Host string `flag:"host" desc:"grpc server host" default:"0.0.0.0"`
	Port int    `flag:"port" desc:"grpc server port" default:"50051"`
}

type Grpc struct {
	config *Config
	server *grpc.Server
}

func New(api api.API, config *Config) api.Subsystem {
	s := &server{service: service.New(api, "grpc")}

	server := grpc.NewServer(grpc.UnaryInterceptor(s.log)) // nosemgrep
	grpcApi.RegisterPromisesServer(server, s)
	grpcApi.RegisterCallbacksServer(server, s)
	grpcApi.RegisterSchedulesServer(server, s)
	grpcApi.RegisterLocksServer(server, s)
	grpcApi.RegisterTasksServer(server, s)

	return &Grpc{
		config: config,
		server: server,
	}
}

func (g *Grpc) String() string {
	return "grpc"
}

func (g *Grpc) Start(errors chan<- error) {
	addr := fmt.Sprintf("%s:%d", g.config.Host, g.config.Port)

	// Create a listener on a specific port
	listen, err := net.Listen("tcp", addr)
	if err != nil {
		errors <- err
		return
	}

	// Start the gRPC server
	slog.Info("starting grpc server", "addr", addr)
	if err := g.server.Serve(listen); err != nil {
		errors <- err
	}
}

func (g *Grpc) Stop() error {
	g.server.GracefulStop()
	return nil
}

type server struct {
	grpcApi.UnimplementedPromisesServer
	grpcApi.UnimplementedCallbacksServer
	grpcApi.UnimplementedSchedulesServer
	grpcApi.UnimplementedLocksServer
	grpcApi.UnimplementedTasksServer
	service *service.Service
}

func (s *server) code(status t_api.StatusCode) codes.Code {
	switch status {
	case
		t_api.StatusOK,
		t_api.StatusCreated,
		t_api.StatusNoContent:
		return codes.OK
	case
		t_api.StatusFieldValidationError:
		return codes.InvalidArgument
	case
		t_api.StatusPromiseAlreadyResolved,
		t_api.StatusPromiseAlreadyRejected,
		t_api.StatusPromiseAlreadyCanceled,
		t_api.StatusPromiseAlreadyTimedout,
		t_api.StatusLockAlreadyAcquired,
		t_api.StatusTaskAlreadyClaimed,
		t_api.StatusTaskAlreadyCompleted,
		t_api.StatusTaskInvalidCounter,
		t_api.StatusTaskInvalidState:
		return codes.PermissionDenied
	case
		t_api.StatusPromiseNotFound,
		t_api.StatusScheduleNotFound,
		t_api.StatusLockNotFound,
		t_api.StatusTaskNotFound:
		return codes.NotFound
	case
		t_api.StatusPromiseAlreadyExists,
		t_api.StatusScheduleAlreadyExists:
		return codes.AlreadyExists

	case
		t_api.StatusInternalServerError,
		t_api.StatusAIOEchoError,
		t_api.StatusAIOQueueError,
		t_api.StatusAIOStoreError:
		return codes.Internal
	case
		t_api.StatusSystemShuttingDown,
		t_api.StatusAPISubmissionQueueFull,
		t_api.StatusAIOSubmissionQueueFull,
		t_api.StatusSchedulerQueueFull:
		return codes.Unavailable

	default:
		panic(fmt.Sprintf("invalid status: %d", status))
	}
}

func (s *server) log(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	res, err := handler(ctx, req)

	slog.Debug("grpc", "method", info.FullMethod, "error", err)
	return res, err
}

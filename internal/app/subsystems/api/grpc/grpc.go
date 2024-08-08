package grpc

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"github.com/resonatehq/resonate/internal/app/subsystems/api/service"

	"github.com/resonatehq/resonate/internal/api"
	grpcApi "github.com/resonatehq/resonate/internal/app/subsystems/api/grpc/api"
	"google.golang.org/grpc"
)

type Config struct {
	Host string
	Port int
}

type Grpc struct {
	config *Config
	server *grpc.Server
}

func New(api api.API, config *Config) api.Subsystem {
	s := &server{service: service.New(api, "grpc")}

	server := grpc.NewServer(grpc.UnaryInterceptor(s.log)) // nosemgrep
	grpcApi.RegisterPromisesServer(server, s)
	grpcApi.RegisterSchedulesServer(server, s)
	grpcApi.RegisterLocksServer(server, s)
	grpcApi.RegisterTasksServer(server, s)

	return &Grpc{
		config: config,
		server: server,
	}
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

func (g *Grpc) String() string {
	return "grpc"
}

type server struct {
	grpcApi.UnimplementedPromisesServer
	grpcApi.UnimplementedSchedulesServer
	grpcApi.UnimplementedLocksServer
	grpcApi.UnimplementedTasksServer
	service *service.Service
}

func (s *server) log(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	res, err := handler(ctx, req)

	slog.Debug("grpc", "method", info.FullMethod, "error", err)
	return res, err
}

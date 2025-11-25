package grpc

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"github.com/go-viper/mapstructure/v2"
	cmdUtil "github.com/resonatehq/resonate/cmd/util"
	i_api "github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/app/subsystems/api"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/grpc/pb"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type Config struct {
	Addr string `flag:"addr" desc:"grpc server address" default:":50051"`
}

func (c *Config) Bind(cmd *cobra.Command, flg *pflag.FlagSet, vip *viper.Viper, name string, prefix string, keyPrefix string) {
	cmdUtil.Bind(c, cmd, flg, vip, name, prefix, keyPrefix)
}

func (c *Config) Decode(value any, decodeHook mapstructure.DecodeHookFunc) error {
	decoderConfig := &mapstructure.DecoderConfig{
		Result:     c,
		DecodeHook: decodeHook,
	}

	decoder, err := mapstructure.NewDecoder(decoderConfig)
	if err != nil {
		return err
	}

	if err := decoder.Decode(value); err != nil {
		return err
	}

	return nil
}

func (c *Config) New(a i_api.API, _ *metrics.Metrics) (i_api.Subsystem, error) {
	return New(a, c)
}

type Grpc struct {
	config *Config
	listen net.Listener
	server *grpc.Server
}

func New(a i_api.API, config *Config) (i_api.Subsystem, error) {
	s := &server{api: api.New(a, "grpc")}

	// Create a listener on specified address
	listen, err := net.Listen("tcp", config.Addr)
	if err != nil {
		return nil, err
	}

	server := grpc.NewServer(grpc.UnaryInterceptor(s.log)) // nosemgrep
	pb.RegisterPromisesServer(server, s)
	pb.RegisterSchedulesServer(server, s)
	pb.RegisterLocksServer(server, s)
	pb.RegisterTasksServer(server, s)

	return &Grpc{
		config: config,
		listen: listen,
		server: server,
	}, nil
}

func (g *Grpc) String() string {
	return "grpc"
}

func (g *Grpc) Kind() string {
	return "grpc"
}

func (g *Grpc) Addr() string {
	return g.listen.Addr().String()
}

func (g *Grpc) Start(errors chan<- error) {
	// Start the grpc server
	slog.Info("starting grpc server", "addr", g.config.Addr)
	if err := g.server.Serve(g.listen); err != nil {
		errors <- err
	}
}

func (g *Grpc) Stop() error {
	g.server.GracefulStop()
	return nil
}

type server struct {
	pb.UnimplementedPromisesServer
	pb.UnimplementedSchedulesServer
	pb.UnimplementedLocksServer
	pb.UnimplementedTasksServer
	api *api.API
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

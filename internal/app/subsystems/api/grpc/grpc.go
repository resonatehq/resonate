package grpc

import (
	"context"
	"log/slog"
	"net"

	"github.com/resonatehq/resonate/internal/app/subsystems/api/service"

	"github.com/resonatehq/resonate/internal/api"
	grpcApi "github.com/resonatehq/resonate/internal/app/subsystems/api/grpc/api"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/pkg/promise"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
)

type Config struct {
	Addr string
}

type Grpc struct {
	config *Config
	server *grpc.Server
}

func New(api api.API, config *Config) api.Subsystem {
	s := &server{service: service.New(api, "grpc")}

	server := grpc.NewServer(grpc.UnaryInterceptor(s.log)) // nosemgrep
	grpcApi.RegisterPromiseServiceServer(server, s)

	return &Grpc{
		config: config,
		server: server,
	}
}

func (g *Grpc) Start(errors chan<- error) {
	// Create a listener on a specific port
	listen, err := net.Listen("tcp", g.config.Addr)
	if err != nil {
		errors <- err
		return
	}

	// Start the gRPC server
	slog.Info("starting grpc server", "addr", g.config.Addr)
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
	grpcApi.UnimplementedPromiseServiceServer
	service *service.Service
}

func (s *server) log(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	res, err := handler(ctx, req)

	slog.Debug("grpc", "method", info.FullMethod, "error", err)
	return res, err
}

func (s *server) ReadPromise(ctx context.Context, req *grpcApi.ReadPromiseRequest) (*grpcApi.ReadPromiseResponse, error) {
	header := &service.Header{
		RequestId: req.RequestId,
	}

	resp, err := s.service.ReadPromise(req.Id, header)
	if err != nil {
		return nil, grpcStatus.Error(codes.Internal, err.Error())
	}

	return &grpcApi.ReadPromiseResponse{
		Status:  protoStatus(resp.Status),
		Promise: protoPromise(resp.Promise),
	}, nil
}

func (s *server) SearchPromises(ctx context.Context, req *grpcApi.SearchPromisesRequest) (*grpcApi.SearchPromisesResponse, error) {
	header := &service.Header{
		RequestId: req.RequestId,
	}

	params := &service.SearchPromiseParams{
		Q:      req.Q,
		State:  searchState(req.State),
		Limit:  int(req.Limit),
		Cursor: req.Cursor,
	}

	resp, err := s.service.SearchPromises(header, params)
	if err != nil {
		if verr, ok := err.(*service.ValidationError); ok {
			return nil, grpcStatus.Error(codes.InvalidArgument, verr.Error())
		} else {
			return nil, grpcStatus.Error(codes.Internal, err.Error())
		}
	}

	promises := make([]*grpcApi.Promise, len(resp.Promises))
	for i, promise := range resp.Promises {
		promises[i] = protoPromise(promise)
	}

	cursor := ""
	if resp.Cursor != nil {
		var err error
		cursor, err = resp.Cursor.Encode()
		if err != nil {
			return nil, grpcStatus.Error(codes.Internal, err.Error())
		}
	}

	return &grpcApi.SearchPromisesResponse{
		Status:   protoStatus(resp.Status),
		Cursor:   cursor,
		Promises: promises,
	}, nil
}

func (s *server) CreatePromise(ctx context.Context, req *grpcApi.CreatePromiseRequest) (*grpcApi.CreatePromiseResponse, error) {
	var idempotencyKey *promise.IdempotencyKey
	if req.IdempotencyKey != "" {
		i := promise.IdempotencyKey(req.IdempotencyKey)
		idempotencyKey = &i
	}

	var headers map[string]string
	if req.Param != nil {
		headers = req.Param.Headers
	}

	var data []byte
	if req.Param != nil {
		data = req.Param.Data
	}

	header := &service.CreatePromiseHeader{
		RequestId:      req.RequestId,
		Strict:         req.Strict,
		IdempotencyKey: idempotencyKey,
	}

	body := &service.CreatePromiseBody{
		Param: promise.Value{
			Headers: headers,
			Data:    data,
		},
		Timeout: req.Timeout,
	}

	resp, err := s.service.CreatePromise(req.Id, header, body)
	if err != nil {
		return nil, grpcStatus.Error(codes.Internal, err.Error())
	}

	return &grpcApi.CreatePromiseResponse{
		Status:  protoStatus(resp.Status),
		Promise: protoPromise(resp.Promise),
	}, nil
}

func (s *server) CancelPromise(ctx context.Context, req *grpcApi.CancelPromiseRequest) (*grpcApi.CancelPromiseResponse, error) {
	var idempotencyKey *promise.IdempotencyKey
	if req.IdempotencyKey != "" {
		i := promise.IdempotencyKey(req.IdempotencyKey)
		idempotencyKey = &i
	}

	var headers map[string]string
	if req.Value != nil {
		headers = req.Value.Headers
	}

	var data []byte
	if req.Value != nil {
		data = req.Value.Data
	}

	header := &service.CancelPromiseHeader{
		RequestId:      req.RequestId,
		Strict:         req.Strict,
		IdempotencyKey: idempotencyKey,
	}

	body := &service.CancelPromiseBody{
		Value: promise.Value{
			Headers: headers,
			Data:    data,
		},
	}
	resp, err := s.service.CancelPromise(req.Id, header, body)
	if err != nil {
		return nil, grpcStatus.Error(codes.Internal, err.Error())
	}

	return &grpcApi.CancelPromiseResponse{
		Status:  protoStatus(resp.Status),
		Promise: protoPromise(resp.Promise),
	}, nil
}

func (s *server) ResolvePromise(ctx context.Context, req *grpcApi.ResolvePromiseRequest) (*grpcApi.ResolvePromiseResponse, error) {
	var idempotencyKey *promise.IdempotencyKey
	if req.IdempotencyKey != "" {
		i := promise.IdempotencyKey(req.IdempotencyKey)
		idempotencyKey = &i
	}

	var headers map[string]string
	if req.Value != nil {
		headers = req.Value.Headers
	}

	var data []byte
	if req.Value != nil {
		data = req.Value.Data
	}

	header := &service.ResolvePromiseHeader{
		RequestId:      req.RequestId,
		Strict:         req.Strict,
		IdempotencyKey: idempotencyKey,
	}

	body := &service.ResolvePromiseBody{
		Value: promise.Value{
			Headers: headers,
			Data:    data,
		},
	}

	resp, err := s.service.ResolvePromise(req.Id, header, body)
	if err != nil {
		return nil, grpcStatus.Error(codes.Internal, err.Error())
	}

	return &grpcApi.ResolvePromiseResponse{
		Status:  protoStatus(resp.Status),
		Promise: protoPromise(resp.Promise),
	}, nil
}

func (s *server) RejectPromise(ctx context.Context, req *grpcApi.RejectPromiseRequest) (*grpcApi.RejectPromiseResponse, error) {
	var idempotencyKey *promise.IdempotencyKey
	if req.IdempotencyKey != "" {
		i := promise.IdempotencyKey(req.IdempotencyKey)
		idempotencyKey = &i
	}

	var headers map[string]string
	if req.Value != nil {
		headers = req.Value.Headers
	}

	var data []byte
	if req.Value != nil {
		data = req.Value.Data
	}

	header := &service.RejectPromiseHeader{
		RequestId:      req.RequestId,
		Strict:         req.Strict,
		IdempotencyKey: idempotencyKey,
	}

	body := &service.RejectPromiseBody{
		Value: promise.Value{
			Headers: headers,
			Data:    data,
		},
	}

	resp, err := s.service.RejectPromise(req.Id, header, body)
	if err != nil {
		return nil, grpcStatus.Error(codes.Internal, err.Error())
	}

	return &grpcApi.RejectPromiseResponse{
		Status:  protoStatus(resp.Status),
		Promise: protoPromise(resp.Promise),
	}, nil
}

func protoStatus(status t_api.ResponseStatus) grpcApi.Status {
	switch status {
	case t_api.ResponseOK:
		return grpcApi.Status_OK
	case t_api.ResponseCreated:
		return grpcApi.Status_CREATED
	case t_api.ResponseNoContent:
		return grpcApi.Status_NOCONTENT
	case t_api.ResponseForbidden:
		return grpcApi.Status_FORBIDDEN
	case t_api.ResponseNotFound:
		return grpcApi.Status_NOTFOUND
	default:
		return grpcApi.Status_UNKNOWN
	}
}

func protoPromise(promise *promise.Promise) *grpcApi.Promise {
	if promise == nil {
		return nil
	}

	var idempotencyKeyForCreate, idempotencyKeyForComplete string
	if promise.IdempotencyKeyForCreate != nil {
		idempotencyKeyForCreate = string(*promise.IdempotencyKeyForCreate)
	}
	if promise.IdempotencyKeyForComplete != nil {
		idempotencyKeyForComplete = string(*promise.IdempotencyKeyForComplete)
	}

	return &grpcApi.Promise{
		Id:    promise.Id,
		State: protoState(promise.State),
		Param: &grpcApi.Value{
			Headers: promise.Param.Headers,
			Data:    promise.Param.Data,
		},
		Value: &grpcApi.Value{
			Headers: promise.Param.Headers,
			Data:    promise.Param.Data,
		},
		Timeout:                   promise.Timeout,
		IdempotencyKeyForCreate:   idempotencyKeyForCreate,
		IdempotencyKeyForComplete: idempotencyKeyForComplete,
	}
}

func protoState(state promise.State) grpcApi.State {
	switch state {
	case promise.Pending:
		return grpcApi.State_PENDING
	case promise.Resolved:
		return grpcApi.State_RESOLVED
	case promise.Rejected:
		return grpcApi.State_REJECTED
	case promise.Timedout:
		return grpcApi.State_REJECTED_TIMEDOUT
	case promise.Canceled:
		return grpcApi.State_REJECTED_CANCELED
	default:
		panic("invalid state")
	}
}

func searchState(searchState grpcApi.SearchState) string {
	switch searchState {
	case grpcApi.SearchState_SEARCH_ALL:
		return ""
	case grpcApi.SearchState_SEARCH_RESOLVED:
		return "resolved"
	case grpcApi.SearchState_SEARCH_REJECTED:
		return "rejected"
	case grpcApi.SearchState_SEARCH_PENDING:
		return "pending"
	default:
		panic("invalid state")
	}
}

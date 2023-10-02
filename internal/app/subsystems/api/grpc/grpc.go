package grpc

import (
	"context"
	"net"

	"log/slog"

	"github.com/resonatehq/resonate/internal/api"
	grpcApi "github.com/resonatehq/resonate/internal/app/subsystems/api/grpc/api"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/resonatehq/resonate/internal/util"
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
	s := &server{api: api}

	server := grpc.NewServer() // nosemgrep
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
	api api.API
}

func (s *server) sendOrPanic(cq chan *bus.CQE[types.Request, types.Response]) func(int64, *types.Response, error) {
	return func(t int64, completion *types.Response, err error) {
		cqe := &bus.CQE[types.Request, types.Response]{
			Kind:       "grpc",
			Completion: completion,
			Error:      err,
		}

		select {
		case cq <- cqe:
		default:
			panic("response channel must not block")
		}
	}
}

func (s *server) ReadPromise(ctx context.Context, req *grpcApi.ReadPromiseRequest) (*grpcApi.ReadPromiseResponse, error) {
	cq := make(chan *bus.CQE[types.Request, types.Response])
	defer close(cq)

	s.api.Enqueue(&bus.SQE[types.Request, types.Response]{
		Kind: "grpc",
		Submission: &types.Request{
			Kind: types.ReadPromise,
			ReadPromise: &types.ReadPromiseRequest{
				Id: req.Id,
			},
		},
		Callback: s.sendOrPanic(cq),
	})

	cqe := <-cq
	if cqe.Error != nil {
		return nil, grpcStatus.Error(codes.Internal, cqe.Error.Error())
	}

	util.Assert(cqe.Completion.ReadPromise != nil, "response must not be nil")

	return &grpcApi.ReadPromiseResponse{
		Status:  protoStatus(cqe.Completion.ReadPromise.Status),
		Promise: protoPromise(cqe.Completion.ReadPromise.Promise),
	}, nil
}

func (s *server) SearchPromises(ctx context.Context, req *grpcApi.SearchPromisesRequest) (*grpcApi.SearchPromisesResponse, error) {
	cq := make(chan *bus.CQE[types.Request, types.Response])
	defer close(cq)

	var searchPromises *types.SearchPromisesRequest

	if req.Cursor != "" {
		cursor, err := types.NewCursor[types.SearchPromisesRequest](req.Cursor)
		if err != nil {
			return nil, grpcStatus.Error(codes.InvalidArgument, err.Error())
		}
		searchPromises = cursor.Next
	} else {
		// validate
		if req.Q == "" {
			return nil, grpcStatus.Error(codes.InvalidArgument, "invalid query")
		}

		var states []promise.State
		switch req.State {
		case grpcApi.SearchState_SEARCH_PENDING:
			states = []promise.State{
				promise.Pending,
			}
		case grpcApi.SearchState_SEARCH_RESOLVED:
			states = []promise.State{
				promise.Resolved,
			}
		case grpcApi.SearchState_SEARCH_REJECTED:
			states = []promise.State{
				promise.Rejected,
				promise.Canceled,
				promise.Timedout,
			}
		default:
			return nil, grpcStatus.Error(codes.InvalidArgument, "invalid state")
		}

		searchPromises = &types.SearchPromisesRequest{
			Q:      req.Q,
			States: states,
			Limit:  int(req.Limit),
		}
	}

	s.api.Enqueue(&bus.SQE[types.Request, types.Response]{
		Kind: "grpc",
		Submission: &types.Request{
			Kind:           types.SearchPromises,
			SearchPromises: searchPromises,
		},
		Callback: s.sendOrPanic(cq),
	})

	cqe := <-cq
	if cqe.Error != nil {
		return nil, grpcStatus.Error(codes.Internal, cqe.Error.Error())
	}

	util.Assert(cqe.Completion.SearchPromises != nil, "response must not be nil")

	promises := make([]*grpcApi.Promise, len(cqe.Completion.SearchPromises.Promises))
	for i, promise := range cqe.Completion.SearchPromises.Promises {
		promises[i] = protoPromise(promise)
	}

	cursor := ""
	if cqe.Completion.SearchPromises.Cursor != nil {
		var err error
		cursor, err = cqe.Completion.SearchPromises.Cursor.Encode()
		if err != nil {
			return nil, grpcStatus.Error(codes.Internal, cqe.Error.Error())
		}

	}

	return &grpcApi.SearchPromisesResponse{
		Status:   protoStatus(cqe.Completion.SearchPromises.Status),
		Cursor:   cursor,
		Promises: promises,
	}, nil
}

func (s *server) CreatePromise(ctx context.Context, req *grpcApi.CreatePromiseRequest) (*grpcApi.CreatePromiseResponse, error) {
	cq := make(chan *bus.CQE[types.Request, types.Response])
	defer close(cq)

	var headers map[string]string
	if req.Param != nil && req.Param.Headers != nil {
		headers = req.Param.Headers
	} else {
		headers = map[string]string{}
	}

	var idempotencyKey *promise.IdempotencyKey
	if req.IdempotencyKey != "" {
		i := promise.IdempotencyKey(req.IdempotencyKey)
		idempotencyKey = &i
	}

	var data []byte
	if req.Param != nil && req.Param.Data != nil {
		data = req.Param.Data
	}

	s.api.Enqueue(&bus.SQE[types.Request, types.Response]{
		Kind: "grpc",
		Submission: &types.Request{
			Kind: types.CreatePromise,
			CreatePromise: &types.CreatePromiseRequest{
				Id: req.Id,
				Param: promise.Value{
					Headers: headers,
					Data:    data,
				},
				Timeout:       req.Timeout,
				IdemptencyKey: idempotencyKey,
				Strict:        req.Strict,
			},
		},
		Callback: s.sendOrPanic(cq),
	})

	cqe := <-cq
	if cqe.Error != nil {
		return nil, grpcStatus.Error(codes.Internal, cqe.Error.Error())
	}

	util.Assert(cqe.Completion.CreatePromise != nil, "response must not be nil")

	return &grpcApi.CreatePromiseResponse{
		Status:  protoStatus(cqe.Completion.CreatePromise.Status),
		Promise: protoPromise(cqe.Completion.CreatePromise.Promise),
	}, nil
}

func (s *server) CancelPromise(ctx context.Context, req *grpcApi.CancelPromiseRequest) (*grpcApi.CancelPromiseResponse, error) {
	cq := make(chan *bus.CQE[types.Request, types.Response])
	defer close(cq)

	var headers map[string]string
	if req.Value != nil && req.Value.Headers != nil {
		headers = req.Value.Headers
	} else {
		headers = map[string]string{}
	}

	var idempotencyKey *promise.IdempotencyKey
	if req.IdempotencyKey != "" {
		i := promise.IdempotencyKey(req.IdempotencyKey)
		idempotencyKey = &i
	}

	var data []byte
	if req.Value != nil && req.Value.Data != nil {
		data = req.Value.Data
	}

	s.api.Enqueue(&bus.SQE[types.Request, types.Response]{
		Kind: "grpc",
		Submission: &types.Request{
			Kind: types.CancelPromise,
			CancelPromise: &types.CancelPromiseRequest{
				Id: req.Id,
				Value: promise.Value{
					Headers: headers,
					Data:    data,
				},
				IdemptencyKey: idempotencyKey,
				Strict:        req.Strict,
			},
		},
		Callback: s.sendOrPanic(cq),
	})

	cqe := <-cq
	if cqe.Error != nil {
		return nil, grpcStatus.Error(codes.Internal, cqe.Error.Error())
	}

	util.Assert(cqe.Completion.CancelPromise != nil, "response must not be nil")

	return &grpcApi.CancelPromiseResponse{
		Status:  protoStatus(cqe.Completion.CancelPromise.Status),
		Promise: protoPromise(cqe.Completion.CancelPromise.Promise),
	}, nil
}

func (s *server) ResolvePromise(ctx context.Context, req *grpcApi.ResolvePromiseRequest) (*grpcApi.ResolvePromiseResponse, error) {
	cq := make(chan *bus.CQE[types.Request, types.Response])
	defer close(cq)

	var headers map[string]string
	if req.Value != nil && req.Value.Headers != nil {
		headers = req.Value.Headers
	} else {
		headers = map[string]string{}
	}

	var idempotencyKey *promise.IdempotencyKey
	if req.IdempotencyKey != "" {
		i := promise.IdempotencyKey(req.IdempotencyKey)
		idempotencyKey = &i
	}

	var data []byte
	if req.Value != nil && req.Value.Data != nil {
		data = req.Value.Data
	}

	s.api.Enqueue(&bus.SQE[types.Request, types.Response]{
		Kind: "grpc",
		Submission: &types.Request{
			Kind: types.ResolvePromise,
			ResolvePromise: &types.ResolvePromiseRequest{
				Id: req.Id,
				Value: promise.Value{
					Headers: headers,
					Data:    data,
				},
				IdemptencyKey: idempotencyKey,
				Strict:        req.Strict,
			},
		},
		Callback: s.sendOrPanic(cq),
	})

	cqe := <-cq
	if cqe.Error != nil {
		return nil, grpcStatus.Error(codes.Internal, cqe.Error.Error())
	}

	util.Assert(cqe.Completion.ResolvePromise != nil, "response must not be nil")

	return &grpcApi.ResolvePromiseResponse{
		Status:  protoStatus(cqe.Completion.ResolvePromise.Status),
		Promise: protoPromise(cqe.Completion.ResolvePromise.Promise),
	}, nil
}

func (s *server) RejectPromise(ctx context.Context, req *grpcApi.RejectPromiseRequest) (*grpcApi.RejectPromiseResponse, error) {
	cq := make(chan *bus.CQE[types.Request, types.Response])
	defer close(cq)

	var headers map[string]string
	if req.Value != nil && req.Value.Headers != nil {
		headers = req.Value.Headers
	} else {
		headers = map[string]string{}
	}

	var idempotencyKey *promise.IdempotencyKey
	if req.IdempotencyKey != "" {
		i := promise.IdempotencyKey(req.IdempotencyKey)
		idempotencyKey = &i
	}

	var data []byte
	if req.Value != nil && req.Value.Data != nil {
		data = req.Value.Data
	}

	s.api.Enqueue(&bus.SQE[types.Request, types.Response]{
		Kind: "grpc",
		Submission: &types.Request{
			Kind: types.RejectPromise,
			RejectPromise: &types.RejectPromiseRequest{
				Id: req.Id,
				Value: promise.Value{
					Headers: headers,
					Data:    data,
				},
				IdemptencyKey: idempotencyKey,
				Strict:        req.Strict,
			},
		},
		Callback: s.sendOrPanic(cq),
	})

	cqe := <-cq
	if cqe.Error != nil {
		return nil, grpcStatus.Error(codes.Internal, cqe.Error.Error())
	}

	util.Assert(cqe.Completion.RejectPromise != nil, "response must not be nil")

	return &grpcApi.RejectPromiseResponse{
		Status:  protoStatus(cqe.Completion.RejectPromise.Status),
		Promise: protoPromise(cqe.Completion.RejectPromise.Promise),
	}, nil
}

func protoStatus(status types.ResponseStatus) grpcApi.Status {
	switch status {
	case types.ResponseOK:
		return grpcApi.Status_OK
	case types.ResponseCreated:
		return grpcApi.Status_CREATED
	case types.ResponseNoContent:
		return grpcApi.Status_NOCONTENT
	case types.ResponseForbidden:
		return grpcApi.Status_FORBIDDEN
	case types.ResponseNotFound:
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

package grpc

import (
	"context"
	"net"

	"log/slog"

	"github.com/resonatehq/resonate/internal/api"
	grpcApi "github.com/resonatehq/resonate/internal/app/subsystems/api/grpc/api"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
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

func (s *server) ReadPromise(ctx context.Context, req *grpcApi.ReadPromiseRequest) (*grpcApi.ReadPromiseResponse, error) {
	cq := make(chan *bus.CQE[t_api.Request, t_api.Response])
	defer close(cq)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Tags: "grpc",
		Submission: &t_api.Request{
			Kind: t_api.ReadPromise,
			ReadPromise: &t_api.ReadPromiseRequest{
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
	cq := make(chan *bus.CQE[t_api.Request, t_api.Response])
	defer close(cq)

	var searchPromises *t_api.SearchPromisesRequest

	if req.Cursor != "" {
		cursor, err := t_api.NewCursor[t_api.SearchPromisesRequest](req.Cursor)
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
		case grpcApi.SearchState_SEARCH_ALL:
			states = []promise.State{
				promise.Pending,
				promise.Resolved,
				promise.Rejected,
				promise.Timedout,
				promise.Canceled,
			}
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
				promise.Timedout,
				promise.Canceled,
			}
		default:
			return nil, grpcStatus.Error(codes.InvalidArgument, "invalid state")
		}

		limit := int(req.Limit)
		if limit <= 0 || limit > 100 {
			limit = 100
		}

		searchPromises = &t_api.SearchPromisesRequest{
			Q:      req.Q,
			States: states,
			Limit:  limit,
		}
	}

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Tags: "grpc",
		Submission: &t_api.Request{
			Kind:           t_api.SearchPromises,
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
	cq := make(chan *bus.CQE[t_api.Request, t_api.Response])
	defer close(cq)

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

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Tags: "grpc",
		Submission: &t_api.Request{
			Kind: t_api.CreatePromise,
			CreatePromise: &t_api.CreatePromiseRequest{
				Id:             req.Id,
				IdempotencyKey: idempotencyKey,
				Strict:         req.Strict,
				Param: promise.Value{
					Headers: headers,
					Data:    data,
				},
				Timeout: req.Timeout,
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
	cq := make(chan *bus.CQE[t_api.Request, t_api.Response])
	defer close(cq)

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

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Tags: "grpc",
		Submission: &t_api.Request{
			Kind: t_api.CancelPromise,
			CancelPromise: &t_api.CancelPromiseRequest{
				Id:             req.Id,
				IdempotencyKey: idempotencyKey,
				Strict:         req.Strict,
				Value: promise.Value{
					Headers: headers,
					Data:    data,
				},
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
	cq := make(chan *bus.CQE[t_api.Request, t_api.Response])
	defer close(cq)

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

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Tags: "grpc",
		Submission: &t_api.Request{
			Kind: t_api.ResolvePromise,
			ResolvePromise: &t_api.ResolvePromiseRequest{
				Id:             req.Id,
				IdempotencyKey: idempotencyKey,
				Strict:         req.Strict,
				Value: promise.Value{
					Headers: headers,
					Data:    data,
				},
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
	cq := make(chan *bus.CQE[t_api.Request, t_api.Response])
	defer close(cq)

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

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Tags: "grpc",
		Submission: &t_api.Request{
			Kind: t_api.RejectPromise,
			RejectPromise: &t_api.RejectPromiseRequest{
				Id:             req.Id,
				IdempotencyKey: idempotencyKey,
				Strict:         req.Strict,
				Value: promise.Value{
					Headers: headers,
					Data:    data,
				},
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

func (s *server) sendOrPanic(cq chan *bus.CQE[t_api.Request, t_api.Response]) func(*t_api.Response, error) {
	return func(completion *t_api.Response, err error) {
		cqe := &bus.CQE[t_api.Request, t_api.Response]{
			Tags:       "grpc",
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

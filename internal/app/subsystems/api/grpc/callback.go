package grpc

import (
	"context"

	grpcApi "github.com/resonatehq/resonate/internal/app/subsystems/api/grpc/api"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/service"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
)

func (s *server) CreateCallback(ctx context.Context, req *grpcApi.CreateCallbackRequest) (*grpcApi.CreateCallbackResponse, error) {
	header := &service.Header{
		RequestId: req.RequestId,
	}

	if req.PromiseId == "" {
		return nil, grpcStatus.Error(codes.InvalidArgument, "callback.promiseId must be provided")
	}
	if req.Recv == "" {
		return nil, grpcStatus.Error(codes.InvalidArgument, "task.recv must be provided")
	}

	body := &service.CreateCallbackBody{
		PromiseId: req.PromiseId,
		Timeout:   req.Timeout,
		Recv:      req.Recv,
		Data:      req.Data,
	}

	_, err := s.service.CreateCallback(header, body)
	if err != nil {
		return nil, grpcStatus.Error(s.code(err.Code), err.Error())
	}

	return &grpcApi.CreateCallbackResponse{}, nil
}

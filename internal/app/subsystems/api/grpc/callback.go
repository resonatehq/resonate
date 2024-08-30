package grpc

import (
	"context"
	"encoding/json"

	grpcApi "github.com/resonatehq/resonate/internal/app/subsystems/api/grpc/api"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/service"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/pkg/callback"
	"github.com/resonatehq/resonate/pkg/message"
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
	if req.Recv == nil {
		return nil, grpcStatus.Error(codes.InvalidArgument, "task.recv must be provided")
	}

	var data interface{}
	if err := json.Unmarshal(req.Recv.Data, &data); err != nil {
		return nil, grpcStatus.Error(codes.InvalidArgument, "task.recv.data must be valid json")
	}

	body := &service.CreateCallbackBody{
		PromiseId: req.PromiseId,
		Timeout:   req.Timeout,
		Recv: &message.Recv{
			Type: req.Recv.Type,
			Data: data,
		},
		Data: req.Data,
	}

	res, err := s.service.CreateCallback(header, body)
	if err != nil {
		return nil, grpcStatus.Error(s.code(err.Code), err.Error())
	}

	return &grpcApi.CreateCallbackResponse{
		Noop:     res.Status == t_api.StatusOK,
		Callback: protoCallback(res.Callback),
		Promise:  protoPromise(res.Promise),
	}, nil
}

func protoCallback(callback *callback.Callback) *grpcApi.Callback {
	if callback == nil {
		return nil
	}

	return &grpcApi.Callback{
		Id:        callback.Id,
		PromiseId: callback.PromiseId,
		Timeout:   callback.Timeout,
		CreatedOn: callback.CreatedOn,
	}
}

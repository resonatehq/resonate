package grpc

import (
	"context"
	"encoding/json"

	grpcApi "github.com/resonatehq/resonate/internal/app/subsystems/api/grpc/api"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/service"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/pkg/callback"
	"github.com/resonatehq/resonate/pkg/receiver"
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

	var recv []byte
	var rErr error
	switch r := req.Recv.(type) {
	case *grpcApi.CreateCallbackRequest_Logical:
		recv, rErr = json.Marshal(&r.Logical)
	case *grpcApi.CreateCallbackRequest_Physical:
		recv, rErr = json.Marshal(&receiver.Recv{Type: r.Physical.Type, Data: r.Physical.Data})
	}

	if rErr != nil {
		return nil, grpcStatus.Error(codes.InvalidArgument, rErr.Error())
	}
	if recv == nil {
		return nil, grpcStatus.Error(codes.InvalidArgument, "callback.recv must be provided")
	}

	body := &service.CreateCallbackBody{
		PromiseId:     req.PromiseId,
		RootPromiseId: req.RootPromiseId,
		Timeout:       req.Timeout,
		Recv:          recv,
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

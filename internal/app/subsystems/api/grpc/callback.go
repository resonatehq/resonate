package grpc

import (
	"context"
	"encoding/json"

	grpcApi "github.com/resonatehq/resonate/internal/app/subsystems/api/grpc/api"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/callback"
	"github.com/resonatehq/resonate/pkg/receiver"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
)

func (s *server) CreateCallback(c context.Context, r *grpcApi.CreateCallbackRequest) (*grpcApi.CreateCallbackResponse, error) {
	var recv []byte
	var rErr error
	switch r := r.Recv.(type) {
	case *grpcApi.CreateCallbackRequest_Logical:
		recv, rErr = json.Marshal(&r.Logical)
	case *grpcApi.CreateCallbackRequest_Physical:
		recv, rErr = json.Marshal(&receiver.Recv{Type: r.Physical.Type, Data: r.Physical.Data})
	}

	if rErr != nil {
		return nil, grpcStatus.Error(codes.InvalidArgument, rErr.Error())
	}

	res, err := s.api.Process(r.RequestId, &t_api.Request{
		Kind: t_api.CreateCallback,
		CreateCallback: &t_api.CreateCallbackRequest{
			PromiseId:     r.PromiseId,
			RootPromiseId: r.RootPromiseId,
			Timeout:       r.Timeout,
			Recv:          recv,
		},
	})
	if err != nil {
		return nil, grpcStatus.Error(s.code(err.Code), err.Error())
	}

	util.Assert(res.CreateCallback != nil, "result must not be nil")
	return &grpcApi.CreateCallbackResponse{
		Noop:     res.CreateCallback.Status == t_api.StatusOK,
		Callback: protoCallback(res.CreateCallback.Callback),
		Promise:  protoPromise(res.CreateCallback.Promise),
	}, nil
}

// Helper functions

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

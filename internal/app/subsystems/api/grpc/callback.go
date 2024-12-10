package grpc

import (
	"context"
	"encoding/json"

	"github.com/resonatehq/resonate/internal/app/subsystems/api/grpc/pb"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/callback"
	"github.com/resonatehq/resonate/pkg/receiver"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) CreateCallback(c context.Context, r *pb.CreateCallbackRequest) (*pb.CreateCallbackResponse, error) {
	recv, rErr := protoRecv(r.Recv)
	if rErr != nil {
		return nil, rErr
	}

	res, err := s.api.Process(r.RequestId, &t_api.Request{
		Kind: t_api.CreateCallback,
		CreateCallback: &t_api.CreateCallbackRequest{
			Id:            r.Id,
			PromiseId:     r.PromiseId,
			RootPromiseId: r.RootPromiseId,
			Timeout:       r.Timeout,
			Recv:          recv,
		},
	})
	if err != nil {
		return nil, status.Error(s.code(err.Code), err.Error())
	}

	util.Assert(res.CreateCallback != nil, "result must not be nil")
	return &pb.CreateCallbackResponse{
		Noop:     res.CreateCallback.Status == t_api.StatusOK,
		Callback: protoCallback(res.CreateCallback.Callback),
		Promise:  protoPromise(res.CreateCallback.Promise),
	}, nil
}

// Helper functions

func protoCallback(callback *callback.Callback) *pb.Callback {
	if callback == nil {
		return nil
	}

	return &pb.Callback{
		Id:        callback.Id,
		PromiseId: callback.PromiseId,
		Timeout:   callback.Timeout,
		CreatedOn: callback.CreatedOn,
	}
}

func protoRecv(recv *pb.Recv) ([]byte, error) {
	switch r := recv.Recv.(type) {
	case *pb.Recv_Logical:
		return json.Marshal(&r.Logical)
	case *pb.Recv_Physical:
		return json.Marshal(&receiver.Recv{Type: r.Physical.Type, Data: r.Physical.Data})
	default:
		return nil, status.Error(codes.InvalidArgument, "The field recv is required.")
	}
}

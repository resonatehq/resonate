package grpc

import (
	"context"

	"github.com/resonatehq/resonate/internal/app/subsystems/api/grpc/pb"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"google.golang.org/grpc/status"
)

func (s *server) CreateNotify(c context.Context, r *pb.CreateNotifyRequest) (*pb.CreateNotifyResponse, error) {
	recv, rErr := protoRecv(r.Recv)
	if rErr != nil {
		return nil, rErr
	}

	res, err := s.api.Process(r.RequestId, &t_api.Request{
		Kind: t_api.CreateNotify,
		CreateNotify: &t_api.CreateNotifyRequest{
			Id:        r.Id,
			PromiseId: r.PromiseId,
			Timeout:   r.Timeout,
			Recv:      recv,
		},
	})
	if err != nil {
		return nil, status.Error(s.code(err.Code), err.Error())
	}

	util.Assert(res.CreateNotify != nil, "result must not be nil")
	return &pb.CreateNotifyResponse{
		Noop:     res.CreateCallback.Status == t_api.StatusOK,
		Callback: protoCallback(res.CreateCallback.Callback),
		Promise:  protoPromise(res.CreateCallback.Promise),
	}, nil
}

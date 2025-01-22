package grpc

import (
	"context"

	"github.com/resonatehq/resonate/internal/app/subsystems/api/grpc/pb"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"google.golang.org/grpc/status"
)

func (s *server) CreateSuscription(c context.Context, r *pb.CreateSuscriptionRequest) (*pb.CreateSuscriptionResponse, error) {
	recv, rErr := protoRecv(r.Recv)
	if rErr != nil {
		return nil, rErr
	}

	res, err := s.api.Process(r.RequestId, &t_api.Request{
		Kind: t_api.CreateSuscription,
		CreateSuscription: &t_api.CreateSuscriptionRequest{
			Id:        r.Id,
			PromiseId: r.PromiseId,
			Timeout:   r.Timeout,
			Recv:      recv,
		},
	})
	if err != nil {
		return nil, status.Error(s.code(err.Code), err.Error())
	}

	util.Assert(res.CreateSuscription != nil, "result must not be nil")
	return &pb.CreateSuscriptionResponse{
		Noop:     res.CreateSuscription.Status == t_api.StatusOK,
		Callback: protoCallback(res.CreateSuscription.Callback),
		Promise:  protoPromise(res.CreateSuscription.Promise),
	}, nil
}

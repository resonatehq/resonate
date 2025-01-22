package grpc

import (
	"context"

	"github.com/resonatehq/resonate/internal/app/subsystems/api/grpc/pb"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"google.golang.org/grpc/status"
)

func (s *server) CreateSubscription(c context.Context, r *pb.CreateSubscriptionRequest) (*pb.CreateSubscriptionResponse, error) {
	recv, rErr := protoRecv(r.Recv)
	if rErr != nil {
		return nil, rErr
	}

	res, err := s.api.Process(r.RequestId, &t_api.Request{
		Kind: t_api.CreateSubscription,
		CreateSubscription: &t_api.CreateSubscriptionRequest{
			Id:        r.Id,
			PromiseId: r.PromiseId,
			Timeout:   r.Timeout,
			Recv:      recv,
		},
	})
	if err != nil {
		return nil, status.Error(s.code(err.Code), err.Error())
	}

	util.Assert(res.CreateSubscription != nil, "result must not be nil")
	return &pb.CreateSubscriptionResponse{
		Noop:     res.CreateSubscription.Status == t_api.StatusOK,
		Callback: protoCallback(res.CreateSubscription.Callback),
		Promise:  protoPromise(res.CreateSubscription.Promise),
	}, nil
}

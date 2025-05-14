package grpc

import (
	"context"

	"github.com/resonatehq/resonate/internal/app/subsystems/api/grpc/pb"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/message"
	"google.golang.org/grpc/status"
)

func (s *server) CreateSubscription(c context.Context, r *pb.CreateSubscriptionRequest) (*pb.CreateSubscriptionResponse, error) {
	recv, rErr := protoRecv(r.Recv)
	if rErr != nil {
		return nil, rErr
	}

	res, err := s.api.Process(r.RequestId, &t_api.Request{
		Kind: t_api.CreateCallback,
		CreateCallback: &t_api.CreateCallbackRequest{
			Id:        s.api.SubscriptionId(r.PromiseId, r.Id),
			PromiseId: r.PromiseId,
			Recv:      recv,
			Mesg:      &message.Mesg{Type: "notify", Root: r.PromiseId},
			Timeout:   r.Timeout,
		},
	})
	if err != nil {
		return nil, status.Error(s.code(err.Code), err.Error())
	}

	util.Assert(res.CreateCallback != nil, "result must not be nil")
	return &pb.CreateSubscriptionResponse{
		Noop:     res.CreateCallback.Status == t_api.StatusOK,
		Callback: protoCallback(res.CreateCallback.Callback),
		Promise:  protoPromise(res.CreateCallback.Promise),
	}, nil
}

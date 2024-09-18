package service

import (
	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
)

type Service struct {
	api      api.API
	protocol string
}

func New(api api.API, protocol string) *Service {
	return &Service{
		api:      api,
		protocol: protocol,
	}
}

func (s *Service) sendOrPanic(id string, cq chan<- *bus.CQE[t_api.Request, t_api.Response]) func(*t_api.Response, error) {
	return func(res *t_api.Response, err error) {
		defer close(cq)

		select {
		case cq <- &bus.CQE[t_api.Request, t_api.Response]{
			Id:         id,
			Completion: res,
			Error:      err,
		}:
		default:
			panic("response channel must not block")
		}
	}
}

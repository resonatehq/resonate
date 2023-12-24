package service

import (
	"github.com/google/uuid"
	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/metadata"
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

func (s *Service) metadata(id string, name string) *metadata.Metadata {
	if id == "" {
		id = uuid.New().String()
	}

	metadata := metadata.New(id)
	metadata.Tags.Set("name", name)
	metadata.Tags.Set("api", s.protocol)

	return metadata
}

func (s *Service) sendOrPanic(cq chan *bus.CQE[t_api.Request, t_api.Response]) func(*t_api.Response, error) {
	return func(completion *t_api.Response, err error) {
		cqe := &bus.CQE[t_api.Request, t_api.Response]{
			// Tags:       s.protocol(),
			Completion: completion,
			Error:      err,
		}

		select {
		case cq <- cqe:
			close(cq) // prevent further writes
		default:
			panic("response channel must not block")
		}
	}
}

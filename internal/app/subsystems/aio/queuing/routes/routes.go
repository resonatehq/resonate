package routes

import (
	"fmt"

	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/routes/pattern"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/routes/t_route"
)

func NewRoute(cfg *t_route.RoutingConfig) (t_route.Route, error) {
	var (
		route t_route.Route
		err   error
	)

	switch cfg.Kind {
	case t_route.Pattern:
		route, err = pattern.New(cfg.Metadata)
	default:
		return nil, fmt.Errorf("invalid routing kind: %s", cfg.Kind)
	}

	if err != nil {
		return nil, err
	}

	return route, nil
}

package routes

import (
	"fmt"

	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/routes/pattern"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/routes/t_route"
)

func NewRoute(cfg *t_route.RoutingConfig) (t_route.Route, error) {
	// Validate all required fields are present.
	if cfg == nil {
		return nil, fmt.Errorf("routing config is nil")
	}
	if cfg.Name == "" {
		return nil, fmt.Errorf("field 'name' is empty")
	}
	if cfg.Kind == "" {
		return nil, fmt.Errorf("field 'kind' is empty for route '%s'", cfg.Name)
	}
	if cfg.Target == nil {
		return nil, fmt.Errorf("field 'target' is empty for route' %s'", cfg.Name)
	}
	if cfg.Target.Connection == "" {
		return nil, fmt.Errorf("field 'target.connection' is empty for route '%s'", cfg.Name)
	}
	if cfg.Target.Queue == "" {
		return nil, fmt.Errorf("field 'target.queue' is empty for route '%s'", cfg.Name)
	}

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

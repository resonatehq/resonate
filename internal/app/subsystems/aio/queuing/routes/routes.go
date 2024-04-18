package routes

import (
	"errors"
	"fmt"

	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/routes/pattern"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/routes/t_route"
)

var (
	ErrMissingRoutingConfig      = errors.New("routing config is nil")
	ErrMissingFieldName          = errors.New("missing field 'name'")
	ErrMissingFieldKind          = errors.New("missing field 'kind'")
	ErrMissingFieldTarget        = errors.New("missing field 'target'")
	ErrMissingFieldConn          = errors.New("missing field 'target.connection'")
	ErrMissingFieldQueue         = errors.New("missing field 'target.queue'")
	ErrMissingMetadata           = errors.New("missing field `metadata`")
	ErrMissingMetadataProperties = errors.New("missing field `metadata.properties`")
	ErrInvalidRoutingKind        = errors.New("invalid routing kind")
)

func NewRoute(cfg *t_route.RoutingConfig) (t_route.Route, error) {
	// Validate all common required fields are present.
	if cfg == nil {
		return nil, ErrMissingRoutingConfig
	}
	if cfg.Name == "" {
		return nil, ErrMissingFieldName
	}
	if cfg.Kind == "" {
		return nil, fmt.Errorf("validation error for route '%s': %w", cfg.Name, ErrMissingFieldKind)
	}
	if cfg.Target == nil {
		return nil, fmt.Errorf("validation error for route '%s': %w", cfg.Name, ErrMissingFieldTarget)
	}
	if cfg.Target.Connection == "" {
		return nil, fmt.Errorf("validation error for route '%s': %w", cfg.Name, ErrMissingFieldConn)
	}
	if cfg.Target.Queue == "" {
		return nil, fmt.Errorf("validation error for route '%s': %w", cfg.Name, ErrMissingFieldQueue)
	}
	if cfg.Metadata == nil {
		return nil, fmt.Errorf("validation error for route '%s': %w", cfg.Name, ErrMissingMetadata)
	}
	if cfg.Metadata.Properties == nil {
		return nil, fmt.Errorf("validation error for route '%s': %w", cfg.Name, ErrMissingMetadataProperties)
	}

	var (
		route t_route.Route
		err   error
	)

	switch cfg.Kind {
	case t_route.Pattern:
		route, err = pattern.New(cfg)
	default:
		return nil, fmt.Errorf("validation error for route '%s': %w", cfg.Name, ErrInvalidRoutingKind)
	}

	if err != nil {
		return nil, err
	}

	return route, nil
}

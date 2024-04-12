package pattern

import (
	"errors"
	"fmt"

	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/metadata"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/routes/t_route"
	"github.com/resonatehq/resonate/internal/util"
)

var (
	ErrMissingPattern = errors.New("missing field 'pattern'")
)

type (
	Pattern struct {
		meta *Metadata
	}

	Metadata struct {
		Pattern string `mapstructure:"pattern"`
	}
)

func New(cfg *t_route.RoutingConfig) (t_route.Route, error) {
	util.Assert(cfg != nil, "routing config must not be nil")
	util.Assert(cfg.Name != "", "name must not be empty")
	util.Assert(cfg.Kind != "", "kind must not be empty")
	util.Assert(cfg.Target != nil, "target must not be nil")
	util.Assert(cfg.Target.Connection != "", "connection must not be empty")
	util.Assert(cfg.Target.Queue != "", "queue must not be empty")
	util.Assert(cfg.Metadata != nil, "metadata must not be nil")
	util.Assert(cfg.Metadata.Properties != nil, "metadata properties must not be nil")

	p := &Pattern{}
	md := &Metadata{}

	if err := metadata.Decode(cfg.Metadata.Properties, md); err != nil {
		return nil, err
	}

	if md.Pattern == "" {
		return nil, fmt.Errorf("validation error for route '%s': %w", cfg.Name, ErrMissingPattern)
	}

	p.meta = md

	util.Assert(p != nil, "pattern must not be nil")
	util.Assert(p.meta != nil, "meta must not be nil")
	util.Assert(p.meta.Pattern != "", "pattern must not be empty")

	return p, nil
}

func (p *Pattern) Route() string {
	return p.meta.Pattern
}

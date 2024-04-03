package pattern

import (
	"fmt"

	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/metadata"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/routes/t_route"
)

type (
	Pattern struct {
		meta Metadata
	}

	Metadata struct {
		Pattern string `mapstructure:"pattern"`
	}
)

func New(meta *metadata.Metadata) (t_route.Route, error) {
	p := &Pattern{}
	md := Metadata{}

	if err := metadata.Decode(meta.Properties, &md); err != nil {
		return nil, err
	}

	p.meta = md

	if p.meta.Pattern == "" {
		return nil, fmt.Errorf("pattern is required")
	}

	return p, nil
}

func (p *Pattern) Route() string {
	return p.meta.Pattern
}

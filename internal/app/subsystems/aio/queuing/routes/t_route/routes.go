package t_route

import (
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/metadata"
)

const (
	Pattern RoutingKind = "pattern"
)

type (
	RoutingKind string

	RoutingConfig struct {
		// Kind identifies the type of routing to use (e.g. pattern, etc.)
		Kind RoutingKind

		// Name identifies the existing routing in the queueing system.
		Name string

		// Target identifies the connection and queue to route the task to.
		Target *Target

		// Metadata is the any additional information or configuration for the specific routing kind.
		Metadata *metadata.Metadata
	}

	Target struct {
		// Connection identifies the connection to route the task to.
		Connection string

		// Queue identifies the task queue within the queueing system.
		Queue string
	}

	Route interface {
		Route() string
	}
)

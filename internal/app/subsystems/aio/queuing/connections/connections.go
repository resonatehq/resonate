package connections

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	http_conn "github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/connections/http"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/connections/t_conn"
	"github.com/resonatehq/resonate/internal/util"
)

var (
	ErrMissingConnectionConfig   = errors.New("connection config is nil")
	ErrMissingFieldName          = errors.New("missing field 'name'")
	ErrMissingFieldKind          = errors.New("missing field 'kind'")
	ErrMissingMetadata           = errors.New("missing field `metadata`")
	ErrMissingMetadataProperties = errors.New("missing field `metadata.properties`")
	ErrInvalidConnectionKind     = errors.New("invalid connection kind")
)

func NewConnection(tasks <-chan *t_conn.ConnectionSubmission, cfg *t_conn.ConnectionConfig) (t_conn.Connection, error) {
	// Validate all common required fields are present.
	if cfg == nil {
		return nil, ErrMissingConnectionConfig
	}
	if cfg.Name == "" {
		return nil, ErrMissingFieldName
	}
	if cfg.Kind == "" {
		return nil, fmt.Errorf("validation error for connection '%s': %w", cfg.Name, ErrMissingFieldKind)
	}
	if cfg.Metadata == nil {
		return nil, fmt.Errorf("validation error for connection '%s': %w", cfg.Name, ErrMissingMetadata)
	}
	if cfg.Metadata.Properties == nil {
		return nil, fmt.Errorf("validation error for connection '%s': %w", cfg.Name, ErrMissingMetadataProperties)
	}

	util.Assert(cfg != nil, "config must not be nil")
	util.Assert(cfg.Name != "", "name must not be empty")
	util.Assert(cfg.Kind != "", "kind must not be empty")
	util.Assert(cfg.Metadata != nil, "metadata must not be nil")
	util.Assert(cfg.Metadata.Properties != nil, "metadata properties must not be nil")

	var (
		conn t_conn.Connection
		err  error
	)

	switch cfg.Kind {
	case t_conn.HTTP:
		param := &http.Client{
			Timeout: 10 * time.Second,
		}
		conn = http_conn.New(param)
		err = conn.Init(tasks, cfg)
	default:
		return nil, fmt.Errorf("validation error for connection '%s': %w", cfg.Name, ErrInvalidConnectionKind)
	}

	if err != nil {
		return nil, err
	}

	util.Assert(conn != nil, "connection must not be nil")

	return conn, nil
}

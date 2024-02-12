package bindings

import (
	"fmt"

	http_binding "github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/bindings/http"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/bindings/t_bind"
)

func NewBindingOrDie(tasks <-chan *t_bind.BindingSubmission, cfg *t_bind.Config) t_bind.Binding {
	var (
		conn t_bind.Binding
		err  error
	)

	switch cfg.Kind {
	case t_bind.HTTP:
		conn = http_binding.New()
		err = conn.Init(tasks, cfg.Metadata)
	default:
		panic(fmt.Sprintf("invalid queuing kind: %s", cfg.Kind))
	}

	if err != nil {
		panic(err)
	}

	return conn
}

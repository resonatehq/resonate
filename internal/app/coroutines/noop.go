package coroutines

import (
	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
)

// Noop coroutine is used for requests that we want to run trough our api middleware but do not have bussiness logic
// e.g. the /poller requests
func Noop(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	return &t_api.Response{
		Status:   t_api.StatusOK,
		Metadata: r.Metadata,
		Payload:  &t_api.NoopResponse{},
	}, nil
}

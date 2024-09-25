package coroutines

import (
	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
)

func Echo(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	submission := &t_aio.Submission{
		Kind: t_aio.Echo,
		Tags: r.Tags,
		Echo: &t_aio.EchoSubmission{
			Data: r.Echo.Data,
		},
	}

	completion, err := gocoro.YieldAndAwait(c, submission)
	if err != nil {
		return nil, t_api.NewError(t_api.StatusAIOEchoError, err)
	}

	return &t_api.Response{
		Kind: t_api.Echo,
		Tags: r.Tags,
		Echo: &t_api.EchoResponse{
			Data: completion.Echo.Data,
		},
	}, nil
}

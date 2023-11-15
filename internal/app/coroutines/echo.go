package coroutines

import (
	"github.com/resonatehq/resonate/internal/kernel/metadata"
	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
)

func Echo(metadata *metadata.Metadata, req *t_api.Request, res func(*t_api.Response, *t_api.PlatformLevelError)) *scheduler.Coroutine[*t_aio.Completion, *t_aio.Submission] {
	return scheduler.NewCoroutine(metadata, func(c *scheduler.Coroutine[*t_aio.Completion, *t_aio.Submission]) {
		submission := &t_aio.Submission{
			Kind: t_aio.Echo,
			Echo: &t_aio.EchoSubmission{
				Data: req.Echo.Data,
			},
		}

		completion, _ := c.Yield(submission)

		res(&t_api.Response{
			Kind: t_api.Echo,
			Echo: &t_api.EchoResponse{
				Data: completion.Echo.Data,
			},
		}, nil)
	})
}

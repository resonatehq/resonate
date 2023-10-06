package coroutines

import (
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
)

func Echo(req *t_api.Request, res func(*t_api.Response, error)) *scheduler.Coroutine {
	return scheduler.NewCoroutine("Echo", func(s *scheduler.Scheduler, c *scheduler.Coroutine) {
		submission := &t_aio.Submission{
			Kind: t_aio.Echo,
			Echo: &t_aio.EchoSubmission{
				Data: req.Echo.Data,
			},
		}

		c.Yield(submission, func(completion *t_aio.Completion, err error) {
			if err != nil {
				slog.Error("echo failed", "err", err)
				return
			}

			util.Assert(completion.Echo != nil, "completion must not be nil")

			res(&t_api.Response{
				Kind: t_api.Echo,
				Echo: &t_api.EchoResponse{
					Data: completion.Echo.Data,
				},
			}, nil)
		})
	})
}

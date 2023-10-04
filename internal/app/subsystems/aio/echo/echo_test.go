package echo

import (
	"testing"

	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/stretchr/testify/assert"
)

func TestEcho(t *testing.T) {
	testCases := []string{"foo", "bar", "baz"}

	for _, tc := range testCases {
		t.Run(tc, func(t *testing.T) {
			sqe := &bus.SQE[t_aio.Submission, t_aio.Completion]{
				Submission: &t_aio.Submission{
					Kind: t_aio.Echo,
					Echo: &t_aio.EchoSubmission{
						Data: tc,
					},
				},
			}

			worker := New().NewWorker(0)
			cqes := worker.Process([]*bus.SQE[t_aio.Submission, t_aio.Completion]{sqe})

			assert.Equal(t, tc, cqes[0].Completion.Echo.Data)
		})
	}
}

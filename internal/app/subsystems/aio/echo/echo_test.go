package echo

import (
	"testing"

	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/stretchr/testify/assert"
)

func TestEcho(t *testing.T) {
	testCases := []string{"foo", "bar", "baz"}

	for _, tc := range testCases {
		t.Run(tc, func(t *testing.T) {
			sqe := &bus.SQE[types.Submission, types.Completion]{
				Submission: &types.Submission{
					Kind: types.Echo,
					Echo: &types.EchoSubmission{
						Data: tc,
					},
				},
			}

			worker := New().NewWorker(0)
			cqes := worker.Process([]*bus.SQE[types.Submission, types.Completion]{sqe})

			assert.Equal(t, tc, cqes[0].Completion.Echo.Data)
		})
	}
}

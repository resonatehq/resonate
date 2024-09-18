package echo

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/stretchr/testify/assert"
)

func TestEcho(t *testing.T) {
	metrics := metrics.New(prometheus.NewRegistry())
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

			echo, err := New(nil, metrics, &Config{Workers: 1})
			assert.Nil(t, err)
			assert.Len(t, echo.workers, 1)

			cqe := echo.workers[0].Process(sqe)
			assert.Equal(t, tc, cqe.Completion.Echo.Data)
		})
	}
}

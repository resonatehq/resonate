package system

import (
	"errors"
	"strconv"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/app/coroutines"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/echo"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/stretchr/testify/assert"
)

func TestSystemLoop(t *testing.T) {
	sq := make(chan *bus.SQE[t_api.Request, t_api.Response], 100)
	cq := make(chan *bus.CQE[t_aio.Submission, t_aio.Completion], 100)
	metrics := metrics.New(prometheus.NewRegistry())

	api := api.New(sq, metrics)
	aio := aio.New(cq, metrics)

	echo, err := echo.New(cq, &echo.Config{Size: 100, BatchSize: 1, Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	aio.AddSubsystem(echo)

	if err := api.Start(); err != nil {
		t.Fatal(err)
	}
	if err := aio.Start(); err != nil {
		t.Fatal(err)
	}

	config := &system.Config{
		CoroutineMaxSize:    100,
		SubmissionBatchSize: 1,
		CompletionBatchSize: 1,
	}

	system := system.New(api, aio, config, metrics)
	system.AddOnRequest(t_api.Echo, coroutines.Echo)

	received := make(chan int, 10)
	defer close(received)

	// all requests made prior to shutdown should succeed
	for i := 0; i < 5; i++ {
		data := strconv.Itoa(i)

		submission := &t_api.Request{
			Kind: t_api.Echo,
			Tags: map[string]string{},
			Echo: &t_api.EchoRequest{
				Data: data,
			},
		}

		api.Enqueue(submission, func(res *t_api.Response, err error) {
			received <- 1

			assert.Nil(t, err)
			assert.Equal(t, data, res.Echo.Data)
		})
	}

	// shutdown
	system.Shutdown()

	// all requests made after shutdown should fail
	for i := 0; i < 5; i++ {
		submission := &t_api.Request{
			Kind: t_api.Echo,
			Tags: map[string]string{},
			Echo: &t_api.EchoRequest{
				Data: "nope",
			},
		}

		api.Enqueue(submission, func(res *t_api.Response, err error) {
			received <- 1

			var apiErr *t_api.ResonateError
			assert.True(t, errors.As(err, &apiErr))
			assert.NotNil(t, err)
			assert.ErrorContains(t, apiErr, "system is shutting down")
		})
	}

	if err := system.Loop(); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		select {
		case <-received:
		default:
			t.Fatal("not enough responses received")
		}
	}

	assert.Zero(t, len(received), "too many responses received")
}

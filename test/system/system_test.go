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
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/stretchr/testify/assert"
)

func TestSystemLoop(t *testing.T) {
	metrics := metrics.New(prometheus.NewRegistry())

	subsystemConfig := &aio.SubsystemConfig{
		Size:      100,
		Workers:   1,
		BatchSize: 1,
	}

	api := api.New(100, metrics)
	aio := aio.New(100, metrics)
	aio.AddSubsystem(t_aio.Echo, echo.New(), subsystemConfig)

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

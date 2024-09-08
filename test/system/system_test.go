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
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/stretchr/testify/assert"
)

func TestSystemLoop(t *testing.T) {
	metrics := metrics.New(prometheus.NewRegistry())

	api := api.New(100, metrics)
	aio := aio.New(100, metrics)

	echo, err := echo.New(aio, metrics, &echo.Config{Size: 100, BatchSize: 1, Workers: 1})
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

		api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
			Submission: &t_api.Request{
				Kind: t_api.Echo,
				Tags: map[string]string{"id": "test"},
				Echo: &t_api.EchoRequest{
					Data: data,
				},
			},
			Callback: func(res *t_api.Response, err error) {
				received <- 1
				assert.Nil(t, err)
				assert.Equal(t, data, res.Echo.Data)
			},
		})
	}

	// shutdown
	system.Shutdown()

	// all requests made after shutdown should fail
	for i := 0; i < 5; i++ {
		api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
			Submission: &t_api.Request{
				Kind: t_api.Echo,
				Tags: map[string]string{"id": "test"},
				Echo: &t_api.EchoRequest{
					Data: "nope",
				},
			},
			Callback: func(res *t_api.Response, err error) {
				received <- 1

				var error *t_api.Error
				assert.True(t, errors.As(err, &error))
				assert.NotNil(t, err)
				assert.ErrorContains(t, error, "system is shutting down")
			},
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

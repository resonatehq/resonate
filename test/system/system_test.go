package system

import (
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
		SubmissionBatchSize: 1,
		CompletionBatchSize: 1,
	}

	system := system.New(api, aio, config, metrics)
	system.AddOnRequest(t_api.Echo, coroutines.Echo)

	recieved := make(chan int, 10)
	defer close(recieved)

	// all requests made prior to shutdown should succeed
	for i := 0; i < 5; i++ {
		data := strconv.Itoa(i)

		api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
			Tags: "test",
			Submission: &t_api.Request{
				Kind: t_api.Echo,
				Echo: &t_api.EchoRequest{
					Data: data,
				},
			},
			Callback: func(res *t_api.Response, err error) {
				recieved <- 1

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
			Tags: "test",
			Submission: &t_api.Request{
				Kind: t_api.Echo,
				Echo: &t_api.EchoRequest{
					Data: "nope",
				},
			},
			Callback: func(res *t_api.Response, err error) {
				recieved <- 1

				assert.NotNil(t, err)
				assert.ErrorContains(t, err, "system is shutting down")
			},
		})
	}

	if err := system.Loop(); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		<-recieved
	}

	assert.Zero(t, len(recieved), "all sqes have been resolved")
}

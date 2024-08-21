package dst

import (
	"math/rand" // nosemgrep
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/app/coroutines"

	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queue"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/sqlite"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/metrics"
)

func TestDST(t *testing.T) {
	dst(t, 0, false, "dst.html")
}

func TestDSTFault(t *testing.T) {
	dst(t, 0.5, false, "dst-fault.html")
}

func TestDSTLazy(t *testing.T) {
	dst(t, 0, true, "dst-lazy.html")
}

func dst(t *testing.T, p float64, l bool, vp string) {
	r := rand.New(rand.NewSource(0))

	// instantiate metrics
	reg := prometheus.NewRegistry()
	metrics := metrics.New(reg)

	// config
	config := &system.Config{
		CoroutineMaxSize:    100,
		SubmissionBatchSize: 100,
		CompletionBatchSize: 100,
		ScheduleBatchSize:   100,
	}

	// sq/cq
	sq := make(chan *bus.SQE[t_api.Request, t_api.Response], 1000)
	// cq := make(chan *bus.CQE[t_aio.Submission, t_aio.Completion], config.AIO.Size)

	// instatiate api/aio
	api := api.New(sq, metrics)
	aio := aio.NewDST(r, p, metrics)

	// instantiate backchannel
	backchannel := make(chan interface{}, 100)

	// instatiate aio subsystems
	queue, err := queue.NewDST(backchannel, &queue.ConfigDST{})
	if err != nil {
		t.Fatal(err)
	}

	store, err := sqlite.New(nil, &sqlite.Config{Path: ":memory:", TxTimeout: 250 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}

	// add api subsystems
	aio.AddSubsystem(store)
	aio.AddSubsystem(queue)

	// instantiate system
	system := system.New(api, aio, config, metrics)
	system.AddOnRequest(t_api.ReadPromise, coroutines.ReadPromise)
	system.AddOnRequest(t_api.SearchPromises, coroutines.SearchPromises)
	system.AddOnRequest(t_api.CreatePromise, coroutines.CreatePromise)
	system.AddOnRequest(t_api.CompletePromise, coroutines.CompletePromise)
	system.AddOnRequest(t_api.CreateCallback, coroutines.CreateCallback)
	system.AddOnRequest(t_api.ReadSchedule, coroutines.ReadSchedule)
	system.AddOnRequest(t_api.SearchSchedules, coroutines.SearchSchedules)
	system.AddOnRequest(t_api.CreateSchedule, coroutines.CreateSchedule)
	system.AddOnRequest(t_api.DeleteSchedule, coroutines.DeleteSchedule)
	system.AddOnRequest(t_api.AcquireLock, coroutines.AcquireLock)
	system.AddOnRequest(t_api.ReleaseLock, coroutines.ReleaseLock)
	system.AddOnRequest(t_api.HeartbeatLocks, coroutines.HeartbeatLocks)
	system.AddOnRequest(t_api.ClaimTask, coroutines.ClaimTask)
	system.AddOnRequest(t_api.CompleteTask, coroutines.CompleteTask)
	system.AddOnRequest(t_api.HeartbeatTasks, coroutines.HeartbeatTasks)

	if !l {
		system.AddBackground("TimeoutPromises", coroutines.TimeoutPromises)
		system.AddBackground("EnqueueTasks", coroutines.EnqueueTasks)
		system.AddBackground("TimeoutTasks", coroutines.TimeoutTasks)

		// TODO: migrate tick to background coroutines
		system.AddOnTick(1*time.Second, "SchedulePromises", coroutines.SchedulePromises)
		system.AddOnTick(1*time.Second, "TimeoutLocks", coroutines.TimeoutLocks)
	}

	// start api/aio
	if err := api.Start(); err != nil {
		t.Fatal(err)
	}
	if err := aio.Start(); err != nil {
		t.Fatal(err)
	}

	ticks := int64(1000)
	timeoutTicks := ticks
	if l {
		timeoutTicks = 5
	}

	dst := New(r, &Config{
		Ticks:              ticks,
		VisualizationPath:  vp,
		TimeElapsedPerTick: 1000, // ms
		TimeoutTicks:       timeoutTicks,
		ReqsPerTick:        func() int { return RangeIntn(r, 0, 15) },
		MaxReqsPerTick:     25,
		Ids:                10,
		IdempotencyKeys:    10,
		Headers:            10,
		Data:               10,
		Tags:               10,
		Searches:           10,
		FaultInjection:     p != 0,
		Backchannel:        backchannel,
	})

	ok := dst.Run(r, api, aio, system)

	// shutdown api/aio
	api.Shutdown()
	aio.Shutdown()

	// stop api/aio
	if err := api.Stop(); err != nil {
		t.Fatal(err)
	}
	if err := aio.Stop(); err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Fatal("DST failed")
	}
}

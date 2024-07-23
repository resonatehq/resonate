package dst

import (
	"math/rand" // nosemgrep
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/app/coroutines"

	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/sqlite"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/metrics"
)

func TestDST(t *testing.T) {
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

	// instatiate api/aio
	api := api.New(1000, metrics)
	aio := aio.NewDST(r, 0, metrics)

	// instatiate aio subsystems
	store, err := sqlite.New(&sqlite.Config{Path: ":memory:", TxTimeout: 250 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}

	// add api subsystems
	aio.AddSubsystem(t_aio.Store, store, nil)

	// instantiate system
	system := system.New(api, aio, config, metrics)
	system.AddOnRequest(t_api.ReadPromise, coroutines.ReadPromise)
	system.AddOnRequest(t_api.SearchPromises, coroutines.SearchPromises)
	system.AddOnRequest(t_api.CreatePromise, coroutines.CreatePromise)
	system.AddOnRequest(t_api.CompletePromise, coroutines.CompletePromise)
	system.AddOnRequest(t_api.ReadSchedule, coroutines.ReadSchedule)
	system.AddOnRequest(t_api.SearchSchedules, coroutines.SearchSchedules)
	system.AddOnRequest(t_api.CreateSchedule, coroutines.CreateSchedule)
	system.AddOnRequest(t_api.DeleteSchedule, coroutines.DeleteSchedule)
	system.AddOnRequest(t_api.AcquireLock, coroutines.AcquireLock)
	system.AddOnRequest(t_api.ReleaseLock, coroutines.ReleaseLock)
	system.AddOnRequest(t_api.HeartbeatLocks, coroutines.HeartbeatLocks)
	system.AddOnTick("SchedulePromises", 1*time.Second, coroutines.SchedulePromises)
	system.AddOnTick("TimeoutPromises", 1*time.Second, coroutines.TimeoutPromises)
	system.AddOnTick("TimeoutLocks", 1*time.Second, coroutines.TimeoutLocks)

	// start api/aio
	if err := api.Start(); err != nil {
		t.Fatal(err)
	}
	if err := aio.Start(); err != nil {
		t.Fatal(err)
	}

	dst := New(r, &Config{
		Ticks:              1000,
		VisualizationPath:  "dst.html",
		TimeElapsedPerTick: 1000, // ms
		ReqsPerTick:        func() int { return RangeIntn(r, 0, 25) },
		MaxReqsPerTick:     25,
		Ids:                100,
		IdempotencyKeys:    100,
		Headers:            100,
		Data:               100,
		Tags:               100,
	})

	ok := dst.Run(r, api, aio, system)

	// shutdown api/aio
	api.Shutdown()
	aio.Shutdown()

	// reset store
	if err := store.Reset(); err != nil {
		t.Fatal(err)
	}

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

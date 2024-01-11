package dst

import (
	"math/rand" // nosemgrep
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/app/coroutines"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/network"
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
		NotificationCacheSize: 100,
		SubmissionBatchSize:   100,
		CompletionBatchSize:   100,
	}

	// instatiate api/aio
	api := api.New(1000, metrics)
	aio := aio.NewDST(r, metrics)

	// instatiate aio subsystems
	network := network.NewDST(&network.ConfigDST{P: 0.5}, r)
	store, err := sqlite.New(&sqlite.Config{Path: ":memory:", TxTimeout: 250 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}

	// add api subsystems
	aio.AddSubsystem(t_aio.Network, network)
	aio.AddSubsystem(t_aio.Store, store)

	// instantiate system
	system := system.New(api, aio, config, metrics)
	system.AddOnRequest(t_api.ReadPromise, coroutines.ReadPromise)
	system.AddOnRequest(t_api.SearchPromises, coroutines.SearchPromises)
	system.AddOnRequest(t_api.CreatePromise, coroutines.CreatePromise)
	system.AddOnRequest(t_api.CancelPromise, coroutines.CancelPromise)
	system.AddOnRequest(t_api.ResolvePromise, coroutines.ResolvePromise)
	system.AddOnRequest(t_api.RejectPromise, coroutines.RejectPromise)
	system.AddOnRequest(t_api.ReadSchedule, coroutines.ReadSchedule)
	system.AddOnRequest(t_api.SearchSchedules, coroutines.SearchSchedules)
	system.AddOnRequest(t_api.CreateSchedule, coroutines.CreateSchedule)
	system.AddOnRequest(t_api.DeleteSchedule, coroutines.DeleteSchedule)
	system.AddOnRequest(t_api.ReadSubscriptions, coroutines.ReadSubscriptions)
	system.AddOnRequest(t_api.CreateSubscription, coroutines.CreateSubscription)
	system.AddOnRequest(t_api.DeleteSubscription, coroutines.DeleteSubscription)
	system.AddOnRequest(t_api.AcquireLock, coroutines.AcquireLock)
	system.AddOnRequest(t_api.BulkHeartbeatLocks, coroutines.BulkHeartbeatLocks)
	system.AddOnRequest(t_api.ReleaseLock, coroutines.ReleaseLock)
	system.AddOnTick(2, coroutines.BulkReleaseLocks)
	system.AddOnTick(2, coroutines.SchedulePromises)
	system.AddOnTick(2, coroutines.TimeoutPromises)
	system.AddOnTick(10, coroutines.NotifySubscriptions)

	// specify reqs to enable
	reqs := []t_api.Kind{
		// PROMISE
		t_api.ReadPromise,
		t_api.SearchPromises,
		t_api.CreatePromise,
		t_api.CancelPromise,
		t_api.ResolvePromise,
		t_api.RejectPromise,

		// SCHEDULE
		t_api.ReadSchedule,
		t_api.SearchSchedules,
		t_api.CreateSchedule,
		t_api.DeleteSchedule,

		// SUBSCRIPTION
		t_api.ReadSubscriptions,
		t_api.CreateSubscription,
		t_api.DeleteSubscription,

		// LOCK
		t_api.AcquireLock,
		t_api.BulkHeartbeatLocks,
		t_api.ReleaseLock,
	}

	// start api/aio
	if err := api.Start(); err != nil {
		t.Fatal(err)
	}
	if err := aio.Start(); err != nil {
		t.Fatal(err)
	}

	dst := New(&Config{
		Ticks:              1000,
		TimeElapsedPerTick: 50_000, // milliseconds
		Reqs:               func() int { return 100 },
		Ids:                100,
		IdempotencyKeys:    100,
		Headers:            100,
		Data:               100,
		Tags:               100,
		Urls:               100,
		Retries:            100,
	})

	if errs := dst.Run(r, api, aio, system, reqs); len(errs) > 0 {
		t.Fatal(errs)
	}

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
}

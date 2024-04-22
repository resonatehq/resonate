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
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing"

	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/sqlite"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/metrics"
)

func TestDST(t *testing.T) {
	test(t, &Scenario{Kind: Default})
}

func TestDSTFaultInjection(t *testing.T) {
	test(t, &Scenario{
		Kind:           FaultInjection,
		FaultInjection: &FaultInjectionScenario{P: 0.5},
	})
}

func test(t *testing.T, scenario *Scenario) {
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

	var _aio aio.AIO
	switch scenario.Kind {
	case FaultInjection:
		_aio = aio.NewDST(r, scenario.FaultInjection.P, metrics)
	default:
		_aio = aio.NewDST(r, 0, metrics)
	}

	// instatiate api/aio
	api := api.New(1000, metrics)
	aio := _aio

	// instatiate aio subsystems
	network := network.NewDST(&network.ConfigDST{P: 0.5}, r)
	store, err := sqlite.New(&sqlite.Config{Path: ":memory:", TxTimeout: 250 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	queuing, err := queuing.NewDST(&queuing.ConfigDST{P: 0.5}, r)
	if err != nil {
		t.Fatal(err)
	}

	// add api subsystems
	aio.AddSubsystem(t_aio.Network, network, nil)
	aio.AddSubsystem(t_aio.Store, store, nil)
	aio.AddSubsystem(t_aio.Queuing, queuing, nil)

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
	system.AddOnRequest(t_api.ReadSubscriptions, coroutines.ReadSubscriptions)
	system.AddOnRequest(t_api.CreateSubscription, coroutines.CreateSubscription)
	system.AddOnRequest(t_api.DeleteSubscription, coroutines.DeleteSubscription)
	system.AddOnRequest(t_api.AcquireLock, coroutines.AcquireLock)
	system.AddOnRequest(t_api.HeartbeatLocks, coroutines.HeartbeatLocks)
	system.AddOnRequest(t_api.ReleaseLock, coroutines.ReleaseLock)
	system.AddOnRequest(t_api.ClaimTask, coroutines.ClaimTask)
	system.AddOnRequest(t_api.CompleteTask, coroutines.CompleteTask)
	system.AddOnTick(2, coroutines.EnqueueTasks)
	system.AddOnTick(2, coroutines.TimeoutLocks)
	system.AddOnTick(2, coroutines.SchedulePromises)
	system.AddOnTick(2, coroutines.TimeoutPromises)
	system.AddOnTick(10, coroutines.NotifySubscriptions)

	// specify reqs to enable
	reqs := []t_api.Kind{
		// PROMISE
		t_api.ReadPromise,
		t_api.SearchPromises,
		t_api.CreatePromise,
		t_api.CompletePromise,

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
		t_api.HeartbeatLocks,
		t_api.ReleaseLock,

		// TASK
		t_api.ClaimTask,
		t_api.CompleteTask,
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
		Scenario:           scenario,
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

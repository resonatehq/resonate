package dst

import (
	"fmt"
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

	for i := 0; i < 2; i++ {
		// config
		config := &system.Config{
			NotificationCacheSize: 100,
			SubmissionBatchSize:   100,
			CompletionBatchSize:   100,
		}

		// instatiate api/aio
		api := api.New(1000, metrics)
		aio := aio.NewDST(r)

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
		system.AddOnRequest(t_api.ReadSubscriptions, coroutines.ReadSubscriptions)
		system.AddOnRequest(t_api.CreateSubscription, coroutines.CreateSubscription)
		system.AddOnRequest(t_api.DeleteSubscription, coroutines.DeleteSubscription)

		var reqs []t_api.Kind
		if i == 0 {
			// promises only
			reqs = []t_api.Kind{
				t_api.ReadPromise,
				t_api.CreatePromise,
				t_api.CancelPromise,
				t_api.ResolvePromise,
				t_api.RejectPromise,
			}

		} else {
			reqs = []t_api.Kind{
				t_api.ReadPromise,
				t_api.SearchPromises,
				t_api.CreatePromise,
				t_api.CancelPromise,
				t_api.ResolvePromise,
				t_api.RejectPromise,
				t_api.ReadSubscriptions,
				t_api.CreateSubscription,
				t_api.DeleteSubscription,
			}

			system.AddOnTick(2, coroutines.TimeoutPromises)
			system.AddOnTick(10, coroutines.NotifySubscriptions)
		}

		// start api/aio
		if err := api.Start(); err != nil {
			t.Fatal(err)
		}
		if err := aio.Start(); err != nil {
			t.Fatal(err)
		}

		dst := New(&Config{
			Ticks:           4000,
			Reqs:            func() int { return 100 },
			Ids:             100,
			IdempotencyKeys: 100,
			Headers:         100,
			Data:            100,
			Tags:            100,
			Urls:            100,
			Retries:         100,
		})

		t.Run(fmt.Sprintf("i=%d, dst=%s", i, dst), func(t *testing.T) {
			if errors := dst.Run(r, api, aio, system, reqs); len(errors) > 0 {
				t.Fatal(errors[0])
			}
		})

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
}

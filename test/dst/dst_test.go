package dst

import (
	"fmt"
	"math/rand" // nosemgrep
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/network"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/sqlite"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/resonatehq/resonate/internal/metrics"
)

func TestDST(t *testing.T) {
	r := rand.New(rand.NewSource(0))

	// instantiate metrics
	reg := prometheus.NewRegistry()
	metrics := metrics.New(reg)

	for i := 0; i < 3; i++ {
		// config
		config := &system.Config{
			TimeoutCacheSize:      i,
			NotificationCacheSize: i,
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
		aio.AddSubsystem(types.Network, network)
		aio.AddSubsystem(types.Store, store)

		// instantiate system
		system := system.New(api, aio, config, metrics)

		// start api/aio
		if err := api.Start(); err != nil {
			t.Fatal(err)
		}
		if err := aio.Start(); err != nil {
			t.Fatal(err)
		}

		dst := New(&Config{
			Ticks:   1000,
			Reqs:    func() int { return 100 },
			Ids:     100,
			Ikeys:   100,
			Data:    100,
			Headers: 100,
			Tags:    100,
			Urls:    100,
			Retries: 100,
		})

		t.Run(fmt.Sprintf("i=%d, dst=%s", i, dst), func(t *testing.T) {
			if errors := dst.Run(r, api, aio, system); len(errors) > 0 {
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

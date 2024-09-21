package sqlite

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/test"
	"github.com/resonatehq/resonate/internal/metrics"
)

func TestSqliteStore(t *testing.T) {
	metrics := metrics.New(prometheus.NewRegistry())

	for _, tc := range test.TestCases {
		store, err := New(nil, metrics, &Config{
			BatchSize: 1,
			Path:      ":memory:",
			TxTimeout: 250 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Start(nil); err != nil {
			t.Fatal(err)
		}

		tc.Run(t, store.worker)

		if err := store.Stop(); err != nil {
			t.Fatal(err)
		}
		if err := store.Reset(); err != nil {
			t.Fatal(err)
		}
	}
}

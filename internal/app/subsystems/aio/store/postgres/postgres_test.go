package postgres

import (
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/test"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/stretchr/testify/assert"
)

func TestPostgresStore(t *testing.T) {
	host := os.Getenv("TEST_AIO_SUBSYSTEMS_STORE_POSTGRES_CONFIG_HOST")
	port := os.Getenv("TEST_AIO_SUBSYSTEMS_STORE_POSTGRES_CONFIG_PORT")
	username := os.Getenv("TEST_AIO_SUBSYSTEMS_STORE_POSTGRES_CONFIG_USERNAME")
	password := os.Getenv("TEST_AIO_SUBSYSTEMS_STORE_POSTGRES_CONFIG_PASSWORD")
	database := os.Getenv("TEST_AIO_SUBSYSTEMS_STORE_POSTGRES_CONFIG_DATABASE")

	if host == "" {
		t.Skip("Postgres is not configured, skipping")
	}

	metrics := metrics.New(prometheus.NewRegistry())

	for _, tc := range test.TestCases {
		store, err := New(nil, metrics, &Config{
			Workers:   1,
			BatchSize: 1,
			Host:      host,
			Port:      port,
			Username:  username,
			Password:  password,
			Database:  database,
			Query:     map[string]string{"sslmode": "disable"},
			TxTimeout: 250 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Start(nil); err != nil {
			t.Fatal(err)
		}

		assert.Len(t, store.workers, 1)
		tc.Run(t, store.workers[0])

		if err := store.Reset(); err != nil {
			t.Fatal(err)
		}

		if err := store.Stop(); err != nil {
			t.Fatal(err)
		}
	}
}

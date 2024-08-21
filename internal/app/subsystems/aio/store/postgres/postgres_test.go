package postgres

import (
	"os"
	"testing"
	"time"

	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/test"
	"github.com/stretchr/testify/assert"
)

func TestPostgresStore(t *testing.T) {
	host := os.Getenv("TEST_AIO_SUBSYSTEMS_STORE_CONFIG_POSTGRES_HOST")
	port := os.Getenv("TEST_AIO_SUBSYSTEMS_STORE_CONFIG_POSTGRES_PORT")
	username := os.Getenv("TEST_AIO_SUBSYSTEMS_STORE_CONFIG_POSTGRES_USERNAME")
	password := os.Getenv("TEST_AIO_SUBSYSTEMS_STORE_CONFIG_POSTGRES_PASSWORD")
	database := os.Getenv("TEST_AIO_SUBSYSTEMS_STORE_CONFIG_POSTGRES_DATABASE")

	if host == "" {
		t.Skip("Postgres is not configured, skipping")
	}

	for _, tc := range test.TestCases {
		store, err := New(nil, &Config{
			Workers:   1,
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

		if err := store.Start(); err != nil {
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

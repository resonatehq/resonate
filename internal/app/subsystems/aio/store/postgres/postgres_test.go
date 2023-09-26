package postgres

import (
	"fmt"
	"os"
	"testing"

	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/test"
)

func TestPostgresStore(t *testing.T) {
	host := withDefault("TEST_AIO_SUBSYSTEMS_STORE_CONFIG_POSTGRES_HOST", "")
	port := withDefault("TEST_AIO_SUBSYSTEMS_STORE_CONFIG_POSTGRES_PORT", "localhost")
	username := withDefault("TEST_AIO_SUBSYSTEMS_STORE_CONFIG_POSTGRES_USERNAME", "username")
	password := withDefault("TEST_AIO_SUBSYSTEMS_STORE_CONFIG_POSTGRES_PASSWORD", "password")
	database := withDefault("TEST_AIO_SUBSYSTEMS_STORE_CONFIG_POSTGRES_DATABASE", "resonate_test")

	if host == "" {
		t.Skip("Postgres is not configured, skipping")
	}

	for i, tc := range test.TestCases {
		store, err := New(&Config{
			Host:     host,
			Port:     port,
			Username: username,
			Password: password,
			Database: database,
			schema:   fmt.Sprintf("test_%d", i),
		}, 1)
		if err != nil {
			t.Fatal(err)
		}

		if err := store.Start(); err != nil {
			t.Fatal(err)
		}

		tc.Run(t, store)

		if err := store.Reset(); err != nil {
			t.Fatal(err)
		}

		if err := store.Stop(); err != nil {
			t.Fatal(err)
		}
	}
}

func withDefault(key, defaultValue string) string {
	value, exists := os.LookupEnv(key)
	if !exists {
		return defaultValue
	}
	return value
}

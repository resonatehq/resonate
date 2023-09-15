package postgres

import (
	"os"
	"testing"

	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/test"
)

func TestPostgresStore(t *testing.T) {
	for _, tc := range test.TestCases {
		host := withDefault("AIO_SUBSYSTEMS_STORE_CONFIG_POSTGRES_HOST", "")
		port := withDefault("AIO_SUBSYSTEMS_STORE_CONFIG_POSTGRES_PORT", "localhost")
		username := withDefault("AIO_SUBSYSTEMS_STORE_CONFIG_POSTGRES_USERNAME", "username")
		password := withDefault("AIO_SUBSYSTEMS_STORE_CONFIG_POSTGRES_PASSWORD", "password")
		database := "resonate_test"

		if host == "" {
			t.Skip("Postgres is not configured, skipping")
		}

		store, err := New(&Config{
			Host:     host,
			Port:     port,
			Username: username,
			Password: password,
			Database: database,
		}, 1)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Start(); err != nil {
			t.Fatal(err)
		}

		tc.Run(t, store)

		if !tc.Panic() {
			if err := store.Reset(); err != nil {
				t.Fatal(err)
			}
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

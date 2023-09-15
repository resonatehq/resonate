package postgres

import (
	"os"
	"testing"

	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/test"
)

func TestPostgresStore(t *testing.T) {
	for _, tc := range test.TestCases {
		host := os.Getenv("POSTGRES_HOST")
		port := os.Getenv("POSTGRES_PORT")
		username := os.Getenv("POSTGRES_USERNAME")
		password := os.Getenv("POSTGRES_PASSWORD")
		database := os.Getenv("POSTGRES_DATABASE")

		if host == "" {
			t.Skip("Postgres is not configured, skipping")
		}

		// temp config
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

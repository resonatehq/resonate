package postgres

import (
	"testing"

	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/test"
)

func TestPostgresStore(t *testing.T) {
	for _, tc := range test.TestCases {
		// temp config
		store, err := New(Config{
			Host:     "localhost",
			Port:     "5432",
			Username: "username",
			Password: "password",
			Database: "resonate",
		})
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

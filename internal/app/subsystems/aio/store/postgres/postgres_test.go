package postgres

import (
	"testing"

	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/test"
)

func TestSqliteStore(t *testing.T) {
	for _, tc := range test.TestCases {
		store, err := New(Config{})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Start(); err != nil {
			t.Fatal(err)
		}

		tc.Run(t, store)

		if err := store.Stop(); err != nil {
			t.Fatal(err)
		}
		if err := store.Reset(); err != nil {
			t.Fatal(err)
		}
	}
}

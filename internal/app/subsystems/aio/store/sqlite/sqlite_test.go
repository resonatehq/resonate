package sqlite

import (
	"testing"
	"time"

	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/test"
)

func TestSqliteStore(t *testing.T) {
	for _, tc := range test.TestCases {
		store, err := New(&Config{
			Path:      ":memory:",
			TxTimeout: 250 * time.Millisecond,
		})
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

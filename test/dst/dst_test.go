package dst

import (
	"fmt"
	"math/rand" // nosemgrep
	"os"
	"strconv"
	"testing"

	"github.com/resonatehq/resonate/test"
)

func TestDST(t *testing.T) {
	seedEnvVar, ok := os.LookupEnv("SEED")
	if !ok {
		seedEnvVar = "0"
	}

	seed, err := strconv.ParseInt(seedEnvVar, 10, 64)
	if err != nil {
		t.FailNow()
	}

	r := rand.New(rand.NewSource(seed))

	// restrict time for ci tests
	var runs int
	var cs func(int) int
	var ticks int64
	if seed == 0 {
		runs = 3
		cs = func(i int) int { return i }
		ticks = 1000
	} else {
		runs = 1
		cs = func(int) int { return test.RangeIntn(r, 0, 1000) }
		ticks = test.RangeInt63n(r, 0, 180000) // one hour (1 tick = 10ms)
	}

	for i := 0; i < runs; i++ {
		dst := &DST{
			Ticks:                 ticks,
			SQEsPerTick:           test.RangeIntn(r, 2, 1000),
			Ids:                   test.RangeIntn(r, 1, 1000000),
			Ikeys:                 test.RangeIntn(r, 1, 100),
			Data:                  test.RangeIntn(r, 1, 100),
			Headers:               test.RangeIntn(r, 1, 100),
			Tags:                  test.RangeIntn(r, 1, 100),
			Urls:                  test.RangeIntn(r, 1, 100),
			Retries:               test.RangeIntn(r, 1, 100),
			PromiseCacheSize:      cs(i),
			TimeoutCacheSize:      cs(i),
			NotificationCacheSize: test.RangeIntn(r, 1, 100),
		}

		ok := t.Run(fmt.Sprintf("seed=%d, dst=%s", seed, dst), func(t *testing.T) {
			dst.Run(t, r, seed)
		})

		if !ok {
			t.FailNow()
		}
	}
}

package dst

import (
	"math/rand"
)

func RangeIntn(r *rand.Rand, min int, max int) int {
	return r.Intn(max-min) + min
}

func RangeInt63n(r *rand.Rand, min int64, max int64) int64 {
	return r.Int63n(max-min) + min
}

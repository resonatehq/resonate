package dst

import (
	"math/rand" // nosemgrep
)

func RangeIntn(r *rand.Rand, min int, max int) int {
	return r.Intn(max-min) + min
}

func RangeInt63n(r *rand.Rand, min int64, max int64) int64 {
	return r.Int63n(max-min) + min
}

func RangeMap[K comparable, V any](r *rand.Rand, m map[K]V) K {
	i := r.Intn(len(m))
	for k := range m {
		if i == 0 {
			return k
		}
		i--
	}
	var zero K
	return zero
}

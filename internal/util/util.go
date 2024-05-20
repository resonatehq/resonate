package util

import (
	"cmp"
	"sort"
	"time"
	"github.com/robfig/cron/v3"
)




func Assert(cond bool, msg string) {
	if !cond {
		panic(msg)
	}
}

type KV[K any, V any] struct {
	Key   K
	Value V
}

func OrderedRange[K cmp.Ordered, V any](m map[K]V) []V {
	sorted := make([]V, len(m))
	for i, key := range orderedRangeSort(m) {
		sorted[i] = m[key]
	}

	return sorted
}

func OrderedRangeKV[K cmp.Ordered, V any](m map[K]V) []*KV[K, V] {
	sorted := make([]*KV[K, V], len(m))
	for i, key := range orderedRangeSort(m) {
		sorted[i] = &KV[K, V]{
			Key:   key,
			Value: m[key],
		}
	}

	return sorted
}

func orderedRangeSort[K cmp.Ordered, V any](m map[K]V) []K {
	keys := make([]K, len(m))

	i := 0
	for key := range m { // nosemgrep: range-over-map
		keys[i] = key
		i++
	}

	sort.SliceStable(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	for i := 0; i < len(keys)-1; i++ {
		Assert(keys[i] <= keys[i+1], "slice not sorted")
	}

	return keys
}

func ToPointer[T any](val T) *T {
	return &val
}

func SafeDeref[T any](val *T) T {
	if val == nil {
		var zero T
		return zero
	}
	return *val
}

func UnixMilliToTime(unixMilli int64) time.Time {
	return time.Unix(0, unixMilli*int64(time.Millisecond))
}

// ref: t := time.Now().UnixMilli()
func Next(curr int64, cronExp string) (int64, error) {
	scheduler, err := ParseCron(cronExp)
	if err != nil {
		return 0, err
	}

	return scheduler.Next(UnixMilliToTime(curr)).UnixMilli(), nil
}

func ParseCron(cronExp string) (cron.Schedule, error) {
	return cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor).Parse(cronExp)
}

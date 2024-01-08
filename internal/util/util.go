package util

import (
	"cmp"
	"sort"
	"strconv"
	"strings"
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
	if isLogicalTimestamp(curr) {
		return nextLogicalTimestamp(curr, cronExp)
	}

	return nextPhysicalTimestamp(curr, cronExp)
}

// isLogicalTimestamp uses some time in the recent past (Jan 6, 2024) to detect if it is
// a logical clock from dst_test.go or a physical clock from system.go.
func isLogicalTimestamp(curr int64) bool {
	return curr < 1_704_586_461_601
}

// nextLogicalTimestamp uses a logical clock for deterministic
// simulation testing, rather than relying on a physical clock.
//
// The smallest units in cron expressions are minutes. Therefore, to
// calculate the next logical clock time, we take the current logical
// time, calculate the next cron expression interval, and add an
// offset of 1 ms/tick per minute.
//
// This allows us to schedule future events and simulations
// deterministically, without needing real time to actually elapse.
// Using a logical clock ensures repeatable and predictable results
// during testing by advancing time predictably in set intervals.
func nextLogicalTimestamp(curr int64, cronExp string) (int64, error) {
	fields := strings.Split(cronExp, " ")
	minutes := fields[0]

	value, err := strconv.Atoi(minutes)
	if err != nil {
		return 0, err
	}

	logical_offset := int64(value)

	next := curr + logical_offset

	return next, nil
}

// nextPhysicalTimestamp uses the actual physical time to calculate the
// next timestamp based on the cron expression.
func nextPhysicalTimestamp(curr int64, cronExp string) (int64, error) {
	scheduler, err := ParseCron(cronExp)
	if err != nil {
		return 0, err
	}

	return scheduler.Next(UnixMilliToTime(curr)).UnixMilli(), nil
}

func ParseCron(cronExp string) (cron.Schedule, error) {
	return cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor).Parse(cronExp)
}

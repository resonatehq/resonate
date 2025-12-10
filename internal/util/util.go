package util

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/robfig/cron/v3"
	"github.com/spf13/viper"
)

func Assert(cond bool, msg string) {
	ignoreAsserts := viper.GetBool("ignore-asserts")
	if !ignoreAsserts && !cond {
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

func Next(curr int64, cronExp string) (int64, error) {
	scheduler, err := ParseCron(cronExp)
	if err != nil {
		return 0, err
	}

	return scheduler.Next(unixMilliToTime(curr)).UnixMilli(), nil
}

func unixMilliToTime(unixMilli int64) time.Time {
	return time.Unix(0, unixMilli*int64(time.Millisecond))
}

func ParseCron(cronExp string) (cron.Schedule, error) {
	return cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor).Parse(cronExp)
}

func UnmarshalChain(data []byte, vs ...any) error {
	var errs []error

	for _, v := range vs {
		err := json.Unmarshal(data, v)
		if err == nil {
			return nil
		}

		// reset v to zero value
		if val := reflect.ValueOf(v); !val.IsNil() {
			val.Elem().Set(reflect.Zero(val.Elem().Type()))
		}

		errs = append(errs, err)
	}

	return errors.Join(errs...)
}

func RemoveWhitespace(s string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		return r
	}, s)
}

func DeferAndLog(f func() error) {
	if err := f(); err != nil {
		slog.Warn("defer failed", "err", err)
	}
}

func InvokeId(promiseId string) string {
	return fmt.Sprintf("__invoke:%s", promiseId)
}

func ResumeId(rootPromiseId, promiseId string) string {
	return fmt.Sprintf("__resume:%s:%s", rootPromiseId, promiseId)
}

func NotifyId(promiseId, id string) string {
	return fmt.Sprintf("__notify:%s:%s", promiseId, id)
}

func ClampAddInt64(a, b int64) int64 {
	if b > 0 && a > math.MaxInt64-b {
		return math.MaxInt64
	}
	return a + b
}

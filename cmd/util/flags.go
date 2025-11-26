package util

import (
	"fmt"
	"math/rand" // nosemgrep
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/mitchellh/mapstructure"
)

type _range interface {
	int | int64 | float64 | time.Duration
}

type rangeFlag[T _range] struct {
	min     T
	max     T
	format  func(T) string
	parse   func(string) (T, error)
	resolve func(*rangeFlag[T], *rand.Rand) T
}

func NewRangeIntFlag(min int, max int) *rangeFlag[int] {
	return &rangeFlag[int]{
		min:     min,
		max:     max,
		format:  func(i int) string { return fmt.Sprintf("%d", i) },
		parse:   func(s string) (int, error) { return strconv.Atoi(s) },
		resolve: func(f *rangeFlag[int], r *rand.Rand) int { return RangeIntn(r, f.min, f.max) },
	}
}

func NewRangeInt64Flag(min int64, max int64) *rangeFlag[int64] {
	return &rangeFlag[int64]{
		min:     min,
		max:     max,
		format:  func(i int64) string { return fmt.Sprintf("%d", i) },
		parse:   func(s string) (int64, error) { return strconv.ParseInt(s, 10, 64) },
		resolve: func(f *rangeFlag[int64], r *rand.Rand) int64 { return RangeInt63n(r, f.min, f.max) },
	}
}

func NewRangeFloat64Flag(min float64, max float64) *rangeFlag[float64] {
	return &rangeFlag[float64]{
		min:     min,
		max:     max,
		format:  func(f float64) string { return fmt.Sprintf("%.2f", f) },
		parse:   func(s string) (float64, error) { return strconv.ParseFloat(s, 64) },
		resolve: func(f *rangeFlag[float64], r *rand.Rand) float64 { return RangeFloat63n(r, f.min, f.max) },
	}
}

func NewRangeDurationFlag(min time.Duration, max time.Duration) *rangeFlag[time.Duration] {
	return &rangeFlag[time.Duration]{
		min:    min,
		max:    max,
		format: func(d time.Duration) string { return d.String() },
		parse: func(s string) (time.Duration, error) {
			d, err := time.ParseDuration(s)
			if err != nil {
				return 0, err
			}
			return d, nil
		},
		resolve: func(f *rangeFlag[time.Duration], r *rand.Rand) time.Duration {
			return time.Duration(RangeInt63n(r, int64(f.min), int64(f.max)))
		},
	}
}

func (f *rangeFlag[T]) String() string {
	return fmt.Sprintf("%s:%s", f.format(f.min), f.format(f.max))
}

func (f *rangeFlag[T]) Type() string {
	return "range"
}

func (f *rangeFlag[T]) Set(s string) error {
	r := strings.Split(s, ":")
	if len(r) != 1 && len(r) != 2 {
		return fmt.Errorf("range flag must contain one or two values separated by a ':'")
	}

	var err error

	f.min, err = f.parse(r[0])
	if err != nil {
		return err
	}

	if len(r) == 2 {
		f.max, err = f.parse(r[1])
		if err != nil {
			return err
		}
	} else {
		f.max = f.min
	}

	return nil
}

func (f *rangeFlag[T]) UnmarshalText(text []byte) error {
	return f.Set(string(text))
}

func (f *rangeFlag[T]) Min() T {
	return f.min
}

func (f *rangeFlag[T]) Max() T {
	return f.max
}

func (f *rangeFlag[T]) Resolve(r *rand.Rand) T {
	if f.min == f.max {
		return f.min
	}

	return f.resolve(f, r)
}

func RangeIntn(r *rand.Rand, min int, max int) int {
	return r.Intn(max-min) + min
}

func RangeInt63n(r *rand.Rand, min int64, max int64) int64 {
	return r.Int63n(max-min) + min
}

func RangeFloat63n(r *rand.Rand, min float64, max float64) float64 {
	return r.Float64()*(max-min) + min
}

func RangeMap[K comparable, V any](r *rand.Rand, m map[K]V) K {
	i := r.Intn(len(m))
	for k := range m { // nosemgrep: range-over-map
		if i == 0 {
			return k
		}
		i--
	}
	var zero K
	return zero
}

// Helper functions

func StringToRange(r *rand.Rand) mapstructure.DecodeHookFunc {
	return func(f reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {
		if f.Kind() != reflect.String {
			return data, nil
		}

		if t.Kind() != reflect.Int && t.Kind() != reflect.Int64 && t.Kind() != reflect.Float64 {
			return data, nil
		}

		raw := data.(string)
		if !strings.Contains(raw, ":") {
			return data, nil
		}

		switch t.Kind() {
		case reflect.Int:
			flag := NewRangeIntFlag(0, 0)
			if err := flag.Set(raw); err != nil {
				return data, err
			}
			return flag.Resolve(r), nil
		case reflect.Int64:
			if t == reflect.TypeOf(time.Duration(0)) {
				flag := NewRangeDurationFlag(0, 0)
				if err := flag.Set(raw); err != nil {
					return data, err
				}
				return flag.Resolve(r), nil
			} else {
				flag := NewRangeInt64Flag(0, 0)
				if err := flag.Set(raw); err != nil {
					return data, err
				}
				return flag.Resolve(r), nil
			}
		case reflect.Float64:
			flag := NewRangeFloat64Flag(0, 0)
			if err := flag.Set(raw); err != nil {
				return data, err
			}
			return flag.Resolve(r), nil
		default:
			panic("unreachable")
		}
	}
}

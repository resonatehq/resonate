package util

import (
	"fmt"
	"math/rand" // nosemgrep
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/resonatehq/resonate/test/dst"
)

type rangeFlag struct {
	Min    int64
	Max    int64
	format func(int64) string
	parse  func(string) (int64, error)
}

func NewRangeIntFlag(min int, max int) *rangeFlag {
	return &rangeFlag{
		Min:    int64(min),
		Max:    int64(max),
		format: func(i int64) string { return fmt.Sprintf("%d", i) },
		parse:  func(s string) (int64, error) { return strconv.ParseInt(s, 10, 64) },
	}
}

func NewRangeInt64Flag(min int64, max int64) *rangeFlag {
	return &rangeFlag{
		Min:    min,
		Max:    max,
		format: func(i int64) string { return fmt.Sprintf("%d", i) },
		parse:  func(s string) (int64, error) { return strconv.ParseInt(s, 10, 64) },
	}
}

func NewRangeDurationFlag(min time.Duration, max time.Duration) *rangeFlag {
	return &rangeFlag{
		Min:    int64(min),
		Max:    int64(max),
		format: func(i int64) string { return time.Duration(i).String() },
		parse: func(s string) (int64, error) {
			d, err := time.ParseDuration(s)
			if err != nil {
				return 0, err
			}
			return int64(d), nil
		},
	}
}

func (f *rangeFlag) String() string {
	if f.Max == f.Min+1 {
		return f.format(f.Min)
	}

	return fmt.Sprintf("%s:%s", f.format(f.Min), f.format(f.Max))
}

func (f *rangeFlag) Type() string {
	return "range"
}

func (f *rangeFlag) Set(s string) error {
	r := strings.Split(s, ":")
	if len(r) != 1 && len(r) != 2 {
		return fmt.Errorf("range flag can contain 1 or 2 values")
	}

	var err error

	f.Min, err = f.parse(r[0])
	if err != nil {
		return err
	}

	if len(r) == 2 {
		f.Max, err = f.parse(r[1])
		if err != nil {
			return err
		}
	} else {
		f.Max = f.Min + 1
	}

	return nil
}

func (f *rangeFlag) UnmarshalText(text []byte) error {
	return f.Set(string(text))
}

func (f *rangeFlag) Int(r *rand.Rand) int {
	return dst.RangeIntn(r, int(f.Min), int(f.Max))
}

func (f *rangeFlag) Int64(r *rand.Rand) int64 {
	return dst.RangeInt63n(r, f.Min, f.Max)
}

func (f *rangeFlag) Duration(r *rand.Rand) time.Duration {
	return time.Duration(dst.RangeInt63n(r, f.Min, f.Max))
}

// Helper functions

func StringToRange(r *rand.Rand) mapstructure.DecodeHookFunc {
	return func(f reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {
		if f.Kind() != reflect.String {
			return data, nil
		}

		if t.Kind() != reflect.Int && t.Kind() != reflect.Int64 {
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
			return flag.Int(r), nil
		case reflect.Int64:
			if t == reflect.TypeOf(time.Duration(0)) {
				flag := NewRangeDurationFlag(0, 0)
				if err := flag.Set(raw); err != nil {
					return data, err
				}
				return flag.Duration(r), nil
			} else {
				flag := NewRangeInt64Flag(0, 0)
				if err := flag.Set(raw); err != nil {
					return data, err
				}
				return flag.Int64(r), nil
			}
		default:
			panic("unreachable")
		}
	}
}

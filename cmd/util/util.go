package util

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand" // nosemgrep
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

func SafeDeref[T any](val *T) T {
	if val == nil {
		var zero T
		return zero
	}
	return *val
}

func PrettyHeaders(headers map[string]string, separator string) []string {
	if headers == nil {
		return []string{}
	}

	result := []string{}
	for _, header := range util.OrderedRangeKV(headers) {
		result = append(result, fmt.Sprintf("%s%s%s", header.Key, separator, header.Value))
	}

	return result
}

func PrettyData(data string) string {
	decoded, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return data
	}

	return string(decoded)
}

func MapToBytes() mapstructure.DecodeHookFunc {
	return func(f reflect.Type, t reflect.Type, data any) (any, error) {
		if f.Kind() != reflect.Map {
			return data, nil
		}

		if t.Kind() != reflect.Slice || t != reflect.TypeOf(json.RawMessage{}) {
			return data, nil
		}

		return json.Marshal(data)
	}
}

// Random value generators
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

func Choose[T any](r *rand.Rand, v ...T) T {
	if len(v) == 0 {
		panic("must provide at least one value")
	}

	return v[r.Intn(len(v))]
}

// Bind configuration struct fields to cobra flags and viper config
func Bind(cfg any, cmd *cobra.Command, flg *pflag.FlagSet, vip *viper.Viper, prefixes ...string) {
	v := reflect.ValueOf(cfg).Elem()
	t := v.Type()

	name := ""
	if len(prefixes) > 0 {
		name = prefixes[0]
	}

	fPrefix := ""
	if len(prefixes) > 1 {
		fPrefix = prefixes[1]
	}

	kPrefix := ""
	if len(prefixes) > 2 {
		kPrefix = prefixes[2]
	}

	for i := range v.NumField() {
		field := t.Field(i)
		flag := field.Tag.Get("flag")
		desc := field.Tag.Get("desc")

		var value string
		if v := field.Tag.Get(name); v != "" {
			value = v
		} else {
			value = field.Tag.Get("default")
		}

		var n string
		if fPrefix == "" {
			n = flag
		} else if flag == "-" {
			n = fPrefix
		} else {
			n = fmt.Sprintf("%s-%s", fPrefix, flag)
		}

		var k string
		if kPrefix == "" {
			k = field.Name
		} else {
			k = fmt.Sprintf("%s.%s", kPrefix, field.Name)
		}

		switch field.Type.Kind() {
		case reflect.String:
			flg.String(n, value, desc)
			_ = vip.BindPFlag(k, flg.Lookup(n))
		case reflect.Bool:
			flg.Bool(n, value == "true", desc)
			_ = vip.BindPFlag(k, flg.Lookup(n))
		case reflect.Int:
			if strings.Contains(value, ":") {
				flag := NewRangeIntFlag(0, 0)
				_ = flag.Set(value)

				flg.Var(flag, n, desc)
				_ = vip.BindPFlag(k, flg.Lookup(n))
			} else {
				v, _ := strconv.Atoi(value)
				flg.Int(n, v, desc)
				_ = vip.BindPFlag(k, flg.Lookup(n))
			}
		case reflect.Int64:
			if field.Type == reflect.TypeOf(time.Duration(0)) {
				if strings.Contains(value, ":") {
					flag := NewRangeDurationFlag(0, 0)
					_ = flag.Set(value)

					flg.Var(flag, n, desc)
					_ = vip.BindPFlag(k, flg.Lookup(n))
				} else {
					v, _ := time.ParseDuration(value)
					flg.Duration(n, v, desc)
					_ = vip.BindPFlag(k, flg.Lookup(n))
				}
			} else {
				if strings.Contains(value, ":") {
					flag := NewRangeInt64Flag(0, 0)
					_ = flag.Set(value)

					flg.Var(flag, n, desc)
					_ = vip.BindPFlag(k, flg.Lookup(n))
				} else {
					v, _ := strconv.ParseInt(value, 10, 64)
					flg.Int64(n, v, desc)
					_ = vip.BindPFlag(k, flg.Lookup(n))
				}
			}
		case reflect.Float64:
			if strings.Contains(value, ":") {
				flag := NewRangeFloat64Flag(0, 0)
				_ = flag.Set(value)

				flg.Var(flag, n, desc)
				_ = vip.BindPFlag(k, flg.Lookup(n))
			} else {
				v, _ := strconv.ParseFloat(value, 64)
				flg.Float64(n, v, desc)
				_ = vip.BindPFlag(k, flg.Lookup(n))
			}
		case reflect.Slice:
			// TODO: support additional slice types
			if field.Type != reflect.TypeOf([]string{}) {
				continue
			}

			var v []string
			if value == "" {
				v = []string{}
			} else {
				v = []string{value}
			}
			flg.StringArray(n, v, desc)
			_ = vip.BindPFlag(k, flg.Lookup(n))
		case reflect.Map:
			// TODO: support additional map types
			if field.Type != reflect.TypeOf(map[string]string{}) {
				continue
			}

			if value == "" {
				value = "{}"
			}
			var v map[string]string
			if err := json.Unmarshal([]byte(value), &v); err != nil {
				panic("invalid json")
			}
			flg.StringToString(n, v, desc)
			_ = vip.BindPFlag(k, flg.Lookup(n))
		case reflect.Struct:
			Bind(v.Field(i).Addr().Interface(), cmd, flg, vip, name, n, k)
		default:
			panic(fmt.Sprintf("unsupported type %s", field.Type.Kind()))
		}
	}
}

// Extract a nested key from viper settings
func Extract(m map[string]any, path string) (any, bool) {
	var cur any = m
	for key := range strings.SplitSeq(path, ".") {
		nextMap, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok = nextMap[key]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}

package util

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
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

// Bind configuration struct fields to cobra flags and viper config
func Bind(cfg any, cmd *cobra.Command, vip *viper.Viper, prefixes ...string) {
	bind(cfg, cmd, cmd.Flags(), vip, prefixes...)
}

func BindPersistent(cfg any, cmd *cobra.Command, vip *viper.Viper, prefixes ...string) {
	bind(cfg, cmd, cmd.PersistentFlags(), vip, prefixes...)
}

func bind(cfg any, cmd *cobra.Command, flags *pflag.FlagSet, vip *viper.Viper, prefixes ...string) {
	v := reflect.ValueOf(cfg).Elem()
	t := v.Type()

	fPrefix := ""
	if len(prefixes) > 0 {
		fPrefix = prefixes[0]
	}

	kPrefix := ""
	if len(prefixes) > 1 {
		kPrefix = prefixes[1]
	}

	for i := range v.NumField() {
		field := t.Field(i)
		flag := field.Tag.Get("flag")
		desc := field.Tag.Get("desc")

		var value string
		if v := field.Tag.Get(cmd.Name()); v != "" {
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
			flags.String(n, value, desc)
			_ = vip.BindPFlag(k, flags.Lookup(n))
		case reflect.Bool:
			flags.Bool(n, value == "true", desc)
			_ = vip.BindPFlag(k, flags.Lookup(n))
		case reflect.Int:
			if strings.Contains(value, ":") {
				flag := NewRangeIntFlag(0, 0)
				_ = flag.Set(value)

				flags.Var(flag, n, desc)
				_ = vip.BindPFlag(k, flags.Lookup(n))
			} else {
				v, _ := strconv.Atoi(value)
				flags.Int(n, v, desc)
				_ = vip.BindPFlag(k, flags.Lookup(n))
			}
		case reflect.Int64:
			if field.Type == reflect.TypeOf(time.Duration(0)) {
				if strings.Contains(value, ":") {
					flag := NewRangeDurationFlag(0, 0)
					_ = flag.Set(value)

					flags.Var(flag, n, desc)
					_ = vip.BindPFlag(k, flags.Lookup(n))
				} else {
					v, _ := time.ParseDuration(value)
					flags.Duration(n, v, desc)
					_ = vip.BindPFlag(k, flags.Lookup(n))
				}
			} else {
				if strings.Contains(value, ":") {
					flag := NewRangeInt64Flag(0, 0)
					_ = flag.Set(value)

					flags.Var(flag, n, desc)
					_ = vip.BindPFlag(k, flags.Lookup(n))
				} else {
					v, _ := strconv.ParseInt(value, 10, 64)
					flags.Int64(n, v, desc)
					_ = vip.BindPFlag(k, flags.Lookup(n))
				}
			}
		case reflect.Float64:
			if strings.Contains(value, ":") {
				flag := NewRangeFloat64Flag(0, 0)
				_ = flag.Set(value)

				flags.Var(flag, n, desc)
				_ = vip.BindPFlag(k, flags.Lookup(n))
			} else {
				v, _ := strconv.ParseFloat(value, 64)
				flags.Float64(n, v, desc)
				_ = vip.BindPFlag(k, flags.Lookup(n))
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
			flags.StringArray(n, v, desc)
			_ = vip.BindPFlag(k, flags.Lookup(n))
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
			flags.StringToString(n, v, desc)
			_ = vip.BindPFlag(k, flags.Lookup(n))
		case reflect.Struct:
			bind(v.Field(i).Addr().Interface(), cmd, flags, vip, n, k)
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

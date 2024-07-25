package util

import (
	"encoding/base64"
	"fmt"

	"github.com/resonatehq/resonate/internal/util"
)

func SafeDeref[T any](val *T) T {
	if val == nil {
		var zero T
		return zero
	}
	return *val
}

func PrettyHeaders(headers map[string]string, seperator string) []string {
	if headers == nil {
		return []string{}
	}

	result := []string{}
	for _, header := range util.OrderedRangeKV(headers) {
		result = append(result, fmt.Sprintf("%s%s%s", header.Key, seperator, header.Value))
	}

	return result
}

func PrettyData(data *string) string {
	if data == nil {
		return ""
	}

	decoded, err := base64.StdEncoding.DecodeString(*data)
	if err != nil {
		return *data
	}

	return string(decoded)
}

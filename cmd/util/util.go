package util

import (
	"encoding/base64"
	"fmt"
)

func SafeDerefToString[T any](val *T) string {
	if val == nil {
		return ""
	}
	return fmt.Sprintf("%v", *val)
}

func PrettyHeaders(headers map[string]string, seperator string) []string {
	if headers == nil {
		return []string{}
	}

	result := []string{}
	for k, v := range headers {
		result = append(result, fmt.Sprintf("%s%s%s", k, seperator, v))
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

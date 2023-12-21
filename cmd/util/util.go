package util

import "fmt"

func SafeDerefToString[T any](val *T) string {
	if val == nil {
		return ""
	}
	return fmt.Sprintf("%v", *val)
}

package util

import "fmt"

func Unwrap[T comparable](value T) *T {
	var zero T
	if value == zero {
		return nil
	}
	return &value
}

func SafeDerefToString[T any](val *T) string {
	if val == nil {
		return ""
	}
	return fmt.Sprintf("%v", *val)
}

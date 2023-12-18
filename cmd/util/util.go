package util

import (
	"fmt"
	"os"
)

func Unwrap[T comparable](value T) *T {
	var zero T
	if value == zero {
		return nil
	}
	return &value
}

func Errorf(format string, v ...any) {
	out := fmt.Sprintf(format, v...)
	fmt.Println(out)
	os.Exit(1)
}

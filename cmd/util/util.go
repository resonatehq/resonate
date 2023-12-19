package util

func Unwrap[T comparable](value T) *T {
	var zero T
	if value == zero {
		return nil
	}
	return &value
}

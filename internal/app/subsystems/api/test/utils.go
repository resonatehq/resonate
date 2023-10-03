package test

import "github.com/resonatehq/resonate/pkg/promise"

func IdempotencyKeyToPointer(s string) *promise.IdempotencyKey {
	idempotencyKey := promise.IdempotencyKey(s)
	return &idempotencyKey
}

func Int64ToPointer(i int64) *int64 {
	return &i
}

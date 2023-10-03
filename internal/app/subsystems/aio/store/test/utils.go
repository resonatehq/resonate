package test

import "github.com/resonatehq/resonate/pkg/promise"

func idempotencyKeyToPointer(s string) *promise.IdempotencyKey {
	idempotencyKey := promise.IdempotencyKey(s)
	return &idempotencyKey
}

func int64ToPointer(i int64) *int64 {
	return &i
}

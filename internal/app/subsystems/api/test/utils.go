package test

import (
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/schedule"
)

func IdempotencyKeyToPointer(s string) *promise.IdempotencyKey {
	idempotencyKey := promise.IdempotencyKey(s)
	return &idempotencyKey
}

func StringToScheduleIkey(s string) *schedule.IdempotencyKey {
	scheduleIkey := schedule.IdempotencyKey(s)
	return &scheduleIkey
}

func Int64ToPointer(i int64) *int64 {
	return &i
}

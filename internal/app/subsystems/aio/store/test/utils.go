package test

import (
	"bytes"
	"encoding/json"

	"github.com/resonatehq/resonate/pkg/promise"
)

func idempotencyKeyToPointer(s string) *promise.IdempotencyKey {
	idempotencyKey := promise.IdempotencyKey(s)
	return &idempotencyKey
}

func int64ToPointer(i int64) *int64 {
	return &i
}

func normalizeJSON(input []byte) []byte {
	if input == nil {
		return nil
	}

	var buf bytes.Buffer
	if err := json.Compact(&buf, input); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

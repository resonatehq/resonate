package test

import (
	"bytes"
	"encoding/json"
)

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

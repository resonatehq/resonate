package receiver

import "encoding/json"

type Recv struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"` // preserve byte array, unmarshal later in plugin
}

func (r *Recv) String() string {
	return r.Type
}

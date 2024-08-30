package message

import (
	"encoding/json"
	"fmt"
)

// Message

type Message struct {
	Recv *Recv  `json:"recv"`
	Data []byte `json:"data"`
}

func (m *Message) String() string {
	return fmt.Sprintf(
		"Message(recv=%s, data=%d)",
		m.Recv,
		len(m.Data),
	)
}

// Recv

type Recv struct {
	Type string      `json:"type"`
	Data interface{} `json:"data"`
}

func (r *Recv) String() string {
	return r.Type
}

func (r *Recv) As(v interface{}) error {
	b, err := json.Marshal(r.Data)
	if err != nil {
		return err
	}

	return json.Unmarshal(b, v)
}

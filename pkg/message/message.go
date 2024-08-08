package message

import "fmt"

type Message struct {
	Recv string `json:"recv"`
	Data []byte `json:"data"`
}

func (m *Message) String() string {
	return fmt.Sprintf(
		"Message(recv=%s, data=%d)",
		m.Recv,
		len(m.Data),
	)
}

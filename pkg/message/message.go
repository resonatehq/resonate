package message

import "fmt"

type Message struct {
	Recv string `json:"recv"`
	Body []byte `json:"body"`
}

func (m *Message) String() string {
	return fmt.Sprintf(
		"Message(recv=%s, body=%d)",
		m.Recv,
		len(m.Body),
	)
}

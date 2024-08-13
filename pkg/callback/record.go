package callback

import (
	"encoding/json"

	"github.com/resonatehq/resonate/pkg/message"
)

type CallbackRecord struct {
	Id        string
	PromiseId string
	Message   []byte
	Timeout   int64
	CreatedOn int64
}

func (r *CallbackRecord) Callback() (*Callback, error) {
	message, err := bytesToMessage(r.Message)
	if err != nil {
		return nil, err
	}

	return &Callback{
		Id:        r.Id,
		PromiseId: r.PromiseId,
		Message:   message,
		Timeout:   r.Timeout,
		CreatedOn: r.CreatedOn,
	}, nil
}

func bytesToMessage(b []byte) (*message.Message, error) {
	var m *message.Message

	if b != nil {
		if err := json.Unmarshal(b, &m); err != nil {
			return nil, err
		}
	}

	return m, nil
}

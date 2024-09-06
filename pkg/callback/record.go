package callback

import "github.com/resonatehq/resonate/pkg/receiver"

type CallbackRecord struct {
	Id        string
	PromiseId string
	RecvType  string
	RecvData  []byte
	Message   []byte
	Timeout   int64
	CreatedOn int64
}

func (r *CallbackRecord) Callback() (*Callback, error) {
	return &Callback{
		Id:        r.Id,
		PromiseId: r.PromiseId,
		Recv:      &receiver.Recv{Type: r.RecvType, Data: r.RecvData},
		Message:   r.Message,
		Timeout:   r.Timeout,
		CreatedOn: r.CreatedOn,
	}, nil
}

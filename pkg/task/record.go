package task

import "github.com/resonatehq/resonate/pkg/receiver"

type TaskRecord struct {
	Id          string
	ProcessId   *string
	State       State
	RecvType    string
	RecvData    []byte
	Message     []byte
	Timeout     int64
	Counter     int
	Attempt     int
	Frequency   int
	Expiration  int64
	CreatedOn   *int64
	CompletedOn *int64
}

func (r *TaskRecord) Task() (*Task, error) {
	return &Task{
		Id:          r.Id,
		ProcessId:   r.ProcessId,
		State:       r.State,
		Recv:        &receiver.Recv{Type: r.RecvType, Data: r.RecvData},
		Message:     r.Message,
		Timeout:     r.Timeout,
		Counter:     r.Counter,
		Attempt:     r.Attempt,
		Frequency:   r.Frequency,
		Expiration:  r.Expiration,
		CreatedOn:   r.CreatedOn,
		CompletedOn: r.CompletedOn,
	}, nil
}

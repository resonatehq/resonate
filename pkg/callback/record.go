package callback

type CallbackRecord struct {
	Id            string
	PromiseId     string
	RootPromiseId string
	Timeout       int64
	Recv          []byte
	CreatedOn     int64
}

func (r *CallbackRecord) Callback() (*Callback, error) {
	return &Callback{
		Id:            r.Id,
		PromiseId:     r.PromiseId,
		RootPromiseId: r.RootPromiseId,
		Timeout:       r.Timeout,
		Recv:          r.Recv,
		CreatedOn:     r.CreatedOn,
	}, nil
}

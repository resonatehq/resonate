package lock

type LockRecord struct {
	ResourceId  string
	ProcessId   string
	ExecutionId string
	Ttl         int64
	ExpiresAt   int64
}

func (r *LockRecord) Lock() (*Lock, error) {
	return &Lock{
		ResourceId:  r.ResourceId,
		ProcessId:   r.ProcessId,
		ExecutionId: r.ExecutionId,
		Ttl:         r.Ttl,
		ExpiresAt:   r.ExpiresAt,
	}, nil
}

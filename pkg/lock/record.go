package lock

type LockRecord struct {
	ResourceId           string
	ProcessId            string
	ExecutionId          string
	ExpiryInMilliseconds int64
	Timeout              int64
}

func (r *LockRecord) Lock() (*Lock, error) {
	return &Lock{
		ResourceId:           r.ResourceId,
		ProcessId:            r.ProcessId,
		ExecutionId:          r.ExecutionId,
		ExpiryInMilliseconds: r.ExpiryInMilliseconds,
		ExpiresAt:            r.Timeout,
	}, nil
}

package lock

type LockRecord struct {
	ResourceId      string
	ProcessId       string
	ExecutionId     string
	ExpiryInSeconds int64
	Timeout         int64
}

func (r *LockRecord) Lock() (*Lock, error) {
	return &Lock{
		ResourceId:      r.ResourceId,
		ProcessId:       r.ProcessId,
		ExecutionId:     r.ExecutionId,
		ExpiryInSeconds: r.ExpiryInSeconds,
		// drop the timeout. it is an implementation detail.
	}, nil
}

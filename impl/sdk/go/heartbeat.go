package resonate

// Heartbeat is the lease-renewal contract Core uses to keep an acquired
// task's TTL alive across long-running executions. Start is called before
// execution begins; Stop is called once execution completes (success, error,
// or panic). Implementations must be safe to call concurrently with distinct
// taskIDs — Core may have many tasks in flight in parallel goroutines.
type Heartbeat interface {
	Start(taskID string, taskVersion int64)
	Stop(taskID string)
}

// NoopHeartbeat is the default Heartbeat for local mode and tests where the
// server does not enforce lease TTL.
type NoopHeartbeat struct{}

func (NoopHeartbeat) Start(string, int64) {}
func (NoopHeartbeat) Stop(string)         {}

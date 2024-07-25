package t_api

type Kind int

const (
	// PROMISES
	ReadPromise Kind = iota
	SearchPromises
	CreatePromise
	CompletePromise

	// SCHEDULES
	ReadSchedule
	SearchSchedules
	CreateSchedule
	DeleteSchedule

	// LOCKS
	AcquireLock
	ReleaseLock
	HeartbeatLocks

	// Echo
	Echo
)

func (k Kind) String() string {
	switch k {
	// PROMISES
	case ReadPromise:
		return "ReadPromise"
	case SearchPromises:
		return "SearchPromises"
	case CreatePromise:
		return "CreatePromise"
	case CompletePromise:
		return "CompletePromise"
	// SCHEDULES
	case ReadSchedule:
		return "ReadSchedule"
	case SearchSchedules:
		return "SearchSchedules"
	case CreateSchedule:
		return "CreateSchedule"
	case DeleteSchedule:
		return "DeleteSchedule"
	// LOCKS
	case AcquireLock:
		return "AcquireLock"
	case ReleaseLock:
		return "ReleaseLock"
	case HeartbeatLocks:
		return "HeartbeatLocks"
	// ECHO
	case Echo:
		return "Echo"
	default:
		panic("invalid api")
	}
}

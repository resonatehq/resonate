package t_api

type Kind int

const (
	// PROMISES
	ReadPromise Kind = iota
	SearchPromises
	CreatePromise
	CreatePromiseAndTask
	CreatePromiseAndCallback
	CompletePromise

	// CALLBACKS
	CreateCallback

	// SCHEDULES
	ReadSchedule
	SearchSchedules
	CreateSchedule
	DeleteSchedule

	// LOCKS
	AcquireLock
	ReleaseLock
	HeartbeatLocks

	// TASKS
	ClaimTask
	CompleteTask
	HeartbeatTasks

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
	case CreatePromiseAndTask:
		return "CreatePromiseAndTask"
	case CreatePromiseAndCallback:
		return "CreatePromiseAndCallback"
	case CompletePromise:
		return "CompletePromise"
	// CALLBACKS
	case CreateCallback:
		return "CreateCallback"
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
	// TASKS
	case ClaimTask:
		return "ClaimTask"
	case CompleteTask:
		return "CompleteTask"
	case HeartbeatTasks:
		return "HeartbeatTasks"
	// ECHO
	case Echo:
		return "Echo"
	default:
		panic("invalid api")
	}
}

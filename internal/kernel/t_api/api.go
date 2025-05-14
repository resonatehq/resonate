package t_api

type Kind int

const (
	// PROMISES
	ReadPromise Kind = iota
	SearchPromises
	CreatePromise
	CreatePromiseAndTask
	CompletePromise

	// CALLBACKS
	CreateCallback

	// SUBSCRIPTIONS
	CreateSubscription

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
	DropTask
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
	case CompletePromise:
		return "CompletePromise"
	// CALLBACKS
	case CreateCallback:
		return "CreateCallback"
	// SUBSCRIPTIONS
	case CreateSubscription:
		return "CreateSubscription"
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
	case DropTask:
		return "DropTask"
	case HeartbeatTasks:
		return "HeartbeatTasks"
	// ECHO
	case Echo:
		return "Echo"
	default:
		panic("invalid api")
	}
}

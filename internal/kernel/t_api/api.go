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

	// SUBSCRIPTIONS
	ReadSubscriptions
	CreateSubscription
	DeleteSubscription

	// LOCKS
	AcquireLock
	HeartbeatLocks
	ReleaseLock

	// TASKS
	ClaimTask
	CompleteTask

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
	// SUBSCRIPTIONS
	case ReadSubscriptions:
		return "ReadSubscriptions"
	case CreateSubscription:
		return "CreateSubscription"
	case DeleteSubscription:
		return "DeleteSubscription"
	// LOCKS
	case AcquireLock:
		return "AcquireLock"
	case HeartbeatLocks:
		return "HeartbeatLocks"
	case ReleaseLock:
		return "ReleaseLock"
	// TASKS
	case ClaimTask:
		return "ClaimTask"
	case CompleteTask:
		return "CompleteTask"
	default:
		panic("invalid api")
	}
}

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
	ReleaseLock
	HeartbeatLocks

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
	case ReleaseLock:
		return "ReleaseLock"
	case HeartbeatLocks:
		return "HeartbeatLocks"
	// TASKS
	case ClaimTask:
		return "ClaimTask"
	case CompleteTask:
		return "CompleteTask"
	// ECHO
	case Echo:
		return "Echo"
	default:
		panic("invalid api")
	}
}

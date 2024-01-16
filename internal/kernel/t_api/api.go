package t_api

type Kind int

const (
	// PROMISES
	ReadPromise Kind = iota
	SearchPromises
	CreatePromise
	CancelPromise
	ResolvePromise
	RejectPromise

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

	// Echo
	Echo
)

func (k Kind) String() string {
	switch k {
	// PROMISES
	case ReadPromise:
		return "read-promise"
	case SearchPromises:
		return "search-promises"
	case CreatePromise:
		return "create-promise"
	case CancelPromise:
		return "cancel-promise"
	case ResolvePromise:
		return "resolve-promise"
	case RejectPromise:
		return "reject-promise"
	// SCHEDULES
	case ReadSchedule:
		return "read-schedule"
	case SearchSchedules:
		return "search-schedules"
	case CreateSchedule:
		return "create-schedule"
	case DeleteSchedule:
		return "delete-schedule"
	// SUBSCRIPTIONS
	case ReadSubscriptions:
		return "read-subscriptions"
	case CreateSubscription:
		return "create-subscription"
	case DeleteSubscription:
		return "delete-subscription"
	// LOCKS
	case AcquireLock:
		return "acquire-lock"
	case HeartbeatLocks:
		return "heartbeat-locks"
	case ReleaseLock:
		return "release-lock"
	default:
		panic("invalid api")
	}
}

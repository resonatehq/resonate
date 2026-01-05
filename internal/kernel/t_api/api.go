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

	// SCHEDULES
	ReadSchedule
	SearchSchedules
	CreateSchedule
	DeleteSchedule

	// TASKS
	ClaimTask
	CompleteTask
	DropTask
	HeartbeatTasks

	// Echo
	Echo

	// Noop
	Noop
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
	// SCHEDULES
	case ReadSchedule:
		return "ReadSchedule"
	case SearchSchedules:
		return "SearchSchedules"
	case CreateSchedule:
		return "CreateSchedule"
	case DeleteSchedule:
		return "DeleteSchedule"
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
	// NOOP
	case Noop:
		return "Noop"
	default:
		panic("invalid api")
	}
}

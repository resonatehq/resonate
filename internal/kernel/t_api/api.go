package t_api

type Kind int

const (
	_ Kind = iota
	// PROMISES
	PromiseGet
	PromiseSearch
	PromiseCreate
	PromiseComplete
	PromiseRegister
	PromiseSubscribe

	// CALLBACKS (old api)
	CallbackCreate

	// SCHEDULES
	ScheduleRead
	ScheduleSearch
	ScheduleCreate
	ScheduleDelete

	// TASKS
	TaskCreate
	TaskAcquire
	TaskRelease
	TaskComplete
	TaskFulfill
	TaskHeartbeat

	// Echo
	Echo

	// Noop
	Noop
)

func (k Kind) String() string {
	switch k {
	// PROMISES
	case PromiseGet:
		return "promise.get"
	case PromiseSearch:
		return "promise.search"
	case PromiseCreate:
		return "promise.create"
	case PromiseComplete:
		return "promise.complete"
	case PromiseRegister:
		return "promise.register"
	case PromiseSubscribe:
		return "promise.subscribe"

	case CallbackCreate:
		return "callback.create"

	// SCHEDULES
	case ScheduleRead:
		return "schedule.get"
	case ScheduleSearch:
		return "schedule.search"
	case ScheduleCreate:
		return "schedule.create"
	case ScheduleDelete:
		return "schedule.delete"
	// TASKS
	case TaskAcquire:
		return "task.acquire"
	case TaskComplete:
		return "task.complete"
	case TaskFulfill:
		return "task.fulfill"
	case TaskRelease:
		return "task.release"
	case TaskHeartbeat:
		return "task.hearbeat"
	case TaskCreate:
		return "task.create"
	// ECHO
	case Echo:
		return "echo"
	// NOOP
	case Noop:
		return "noop"
	default:
		panic("invalid api")
	}
}

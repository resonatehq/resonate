package t_aio

import (
	"fmt"

	"github.com/resonatehq/resonate/pkg/callback"
	"github.com/resonatehq/resonate/pkg/idempotency"
	"github.com/resonatehq/resonate/pkg/lock"
	"github.com/resonatehq/resonate/pkg/message"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/schedule"
	"github.com/resonatehq/resonate/pkg/task"
)

type StoreKind int

const (
	// PROMISES
	ReadPromise StoreKind = iota
	SearchPromises
	CreatePromise
	UpdatePromise
	TimeoutPromises

	// CALLBACKS
	ReadCallbacks
	CreateCallback
	DeleteCallbacks

	// SCHEDULES
	ReadSchedule
	ReadSchedules
	SearchSchedules
	CreateSchedule
	UpdateSchedule
	DeleteSchedule

	// TASKS
	ReadTask
	ReadTasks
	CreateTasks
	UpdateTask
	HeartbeatTask

	// LOCKS
	ReadLock
	AcquireLock
	ReleaseLock
	HeartbeatLocks
	TimeoutLocks
)

func (k StoreKind) String() string {
	switch k {
	// PROMISES
	case ReadPromise:
		return "ReadPromise"
	case SearchPromises:
		return "SearchPromises"
	case CreatePromise:
		return "CreatePromise"
	case UpdatePromise:
		return "UpdatePromise"
	case TimeoutPromises:
		return "TimeoutPromises"
	// CALLBACKS
	case ReadCallbacks:
		return "ReadCallbacks"
	case CreateCallback:
		return "CreateCallback"
	case DeleteCallbacks:
		return "DeleteCallbacks"
	// SCHEDULES
	case ReadSchedule:
		return "ReadSchedule"
	case ReadSchedules:
		return "ReadSchedules"
	case SearchSchedules:
		return "SearchSchedules"
	case CreateSchedule:
		return "CreateSchedule"
	case UpdateSchedule:
		return "UpdateSchedule"
	case DeleteSchedule:
		return "DeleteSchedule"
	// TASKS
	case ReadTask:
		return "ReadTask"
	case ReadTasks:
		return "ReadTasks"
	case CreateTasks:
		return "CreateTasks"
	case UpdateTask:
		return "UpdateTask"
	case HeartbeatTask:
		return "HeartbeatTask"
	// LOCKS
	case ReadLock:
		return "ReadLock"
	case AcquireLock:
		return "AcquireLock"
	case ReleaseLock:
		return "ReleaseLock"
	case HeartbeatLocks:
		return "HeartbeatLocks"
	case TimeoutLocks:
		return "TimeoutLocks"

	default:
		panic("invalid store kind")
	}
}

type StoreSubmission struct {
	Transaction *Transaction
}

func (s *StoreSubmission) String() string {
	return fmt.Sprintf("Store(transaction=Transaction(commands=%s))", s.Transaction.Commands)
}

type StoreCompletion struct {
	Results []*Result
}

func (c *StoreCompletion) String() string {
	return fmt.Sprintf("Store(results=%s)", c.Results)
}

type Transaction struct {
	Commands []*Command
}

type Command struct {
	Kind StoreKind

	// PROMISES
	ReadPromise     *ReadPromiseCommand
	SearchPromises  *SearchPromisesCommand
	CreatePromise   *CreatePromiseCommand
	UpdatePromise   *UpdatePromiseCommand
	TimeoutPromises *TimeoutPromisesCommand

	// CALLBACKS
	ReadCallbacks   *ReadCallbacksCommand
	CreateCallback  *CreateCallbackCommand
	DeleteCallbacks *DeleteCallbacksCommand

	// SCHEDULES
	ReadSchedule    *ReadScheduleCommand
	ReadSchedules   *ReadSchedulesCommand
	SearchSchedules *SearchSchedulesCommand
	CreateSchedule  *CreateScheduleCommand
	UpdateSchedule  *UpdateScheduleCommand
	DeleteSchedule  *DeleteScheduleCommand

	// TASKS
	ReadTask      *ReadTaskCommand
	ReadTasks     *ReadTasksCommand
	CreateTasks   *CreateTasksCommand
	UpdateTask    *UpdateTaskCommand
	HeartbeatTask *HeartbeatTaskCommand

	// LOCKS
	ReadLock       *ReadLockCommand
	AcquireLock    *AcquireLockCommand
	ReleaseLock    *ReleaseLockCommand
	HeartbeatLocks *HeartbeatLocksCommand
	TimeoutLocks   *TimeoutLocksCommand
}

func (c *Command) String() string {
	return c.Kind.String()
}

type Result struct {
	Kind StoreKind

	// PROMISES
	ReadPromise     *QueryPromisesResult
	SearchPromises  *QueryPromisesResult
	CreatePromise   *AlterPromisesResult
	UpdatePromise   *AlterPromisesResult
	TimeoutPromises *AlterPromisesResult

	// CALLBACKS
	ReadCallbacks   *QueryCallbacksResult
	CreateCallback  *AlterCallbacksResult
	DeleteCallbacks *AlterCallbacksResult

	// SCHEDULES
	ReadSchedule    *QuerySchedulesResult
	ReadSchedules   *QuerySchedulesResult
	SearchSchedules *QuerySchedulesResult
	CreateSchedule  *AlterSchedulesResult
	UpdateSchedule  *AlterSchedulesResult
	DeleteSchedule  *AlterSchedulesResult

	// TASKS
	ReadTask      *QueryTasksResult
	ReadTasks     *QueryTasksResult
	CreateTasks   *AlterTasksResult
	UpdateTask    *AlterTasksResult
	HeartbeatTask *AlterTasksResult

	// LOCKS
	ReadLock       *QueryLocksResult
	AcquireLock    *AlterLocksResult
	ReleaseLock    *AlterLocksResult
	HeartbeatLocks *AlterLocksResult
	TimeoutLocks   *AlterLocksResult
}

func (r *Result) String() string {
	return r.Kind.String()
}

// Promise commands

type ReadPromiseCommand struct {
	Id string
}

type SearchPromisesCommand struct {
	Id     string
	States []promise.State
	Tags   map[string]string
	Limit  int
	SortId *int64
}

type CreatePromiseCommand struct {
	Id             string
	State          promise.State
	Param          promise.Value
	Timeout        int64
	IdempotencyKey *idempotency.Key
	Tags           map[string]string
	CreatedOn      int64
}

type UpdatePromiseCommand struct {
	Id             string
	State          promise.State
	Value          promise.Value
	IdempotencyKey *idempotency.Key
	CompletedOn    int64
}

type TimeoutPromisesCommand struct {
	Time int64
}

// Promise results

type QueryPromisesResult struct {
	RowsReturned int64
	LastSortId   int64
	Records      []*promise.PromiseRecord
}

type AlterPromisesResult struct {
	RowsAffected int64
}

// Callback commands

type ReadCallbacksCommand struct {
	PromiseId string
}

type CreateCallbackCommand struct {
	PromiseId string
	Message   *message.Message
	Timeout   int64
	CreatedOn int64
}

type DeleteCallbacksCommand struct {
	PromiseId string
}

// Callback results

type QueryCallbacksResult struct {
	RowsReturned int64
	Records      []*callback.CallbackRecord
}

type AlterCallbacksResult struct {
	RowsAffected int64
	LastInsertId int64
}

// Schedule commands

type ReadScheduleCommand struct {
	Id string
}

type ReadSchedulesCommand struct {
	NextRunTime int64
	Limit       int64
}

type SearchSchedulesCommand struct {
	Id     string
	Tags   map[string]string
	Limit  int
	SortId *int64
}

type CreateScheduleCommand struct {
	Id             string
	Description    string
	Cron           string
	Tags           map[string]string
	PromiseId      string
	PromiseTimeout int64
	PromiseParam   promise.Value
	PromiseTags    map[string]string
	NextRunTime    int64
	IdempotencyKey *idempotency.Key
	CreatedOn      int64
}

type UpdateScheduleCommand struct {
	Id          string
	LastRunTime *int64
	NextRunTime int64
}

type DeleteScheduleCommand struct {
	Id string
}

// Schedule results

type QuerySchedulesResult struct {
	RowsReturned int64
	LastSortId   int64
	Records      []*schedule.ScheduleRecord
}

type AlterSchedulesResult struct {
	RowsAffected int64
}

// Task commands

type ReadTaskCommand struct {
	Id int64
}

type ReadTasksCommand struct {
	States []task.State
	Time   int64
	Limit  int64
}

type CreateTasksCommand struct {
	PromiseId string
	CreatedOn int64
}

type UpdateTaskCommand struct {
	Id             int64
	State          task.State
	Counter        int
	Frequency      int
	Expiration     int64
	CompletedOn    *int64
	CurrentStates  []task.State
	CurrentCounter int
}

type HeartbeatTaskCommand struct {
	Id      int64
	Counter int
	Time    int64
}

// Task results

type QueryTasksResult struct {
	RowsReturned int64
	Records      []*task.TaskRecord
}

type AlterTasksResult struct {
	RowsAffected int64
}

// Lock commands

type ReadLockCommand struct {
	ResourceId string
}

type AcquireLockCommand struct {
	ResourceId           string
	ProcessId            string
	ExecutionId          string
	ExpiryInMilliseconds int64
	Timeout              int64
}

type ReleaseLockCommand struct {
	ResourceId  string
	ExecutionId string
}

type HeartbeatLocksCommand struct {
	ProcessId string
	Time      int64
}

type TimeoutLocksCommand struct {
	Timeout int64
}

// Lock results

type QueryLocksResult struct {
	RowsReturned int64
	Records      []*lock.LockRecord
}

type AlterLocksResult struct {
	RowsAffected int64
}

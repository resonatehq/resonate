package t_aio

import (
	"fmt"

	"github.com/resonatehq/resonate/pkg/idempotency"
	"github.com/resonatehq/resonate/pkg/lock"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/schedule"
)

type StoreKind int

const (
	// PROMISES
	ReadPromise StoreKind = iota
	SearchPromises
	CreatePromise
	UpdatePromise
	TimeoutPromises

	// SCHEDULES
	ReadSchedule
	ReadSchedules
	SearchSchedules
	CreateSchedule
	UpdateSchedule
	DeleteSchedule

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

	// SCHEDULES
	ReadSchedule    *ReadScheduleCommand
	ReadSchedules   *ReadSchedulesCommand
	SearchSchedules *SearchSchedulesCommand
	CreateSchedule  *CreateScheduleCommand
	UpdateSchedule  *UpdateScheduleCommand
	DeleteSchedule  *DeleteScheduleCommand

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

	// SCHEDULES
	ReadSchedule    *QuerySchedulesResult
	ReadSchedules   *QuerySchedulesResult
	SearchSchedules *QuerySchedulesResult
	CreateSchedule  *AlterSchedulesResult
	UpdateSchedule  *AlterSchedulesResult
	DeleteSchedule  *AlterSchedulesResult

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

package t_aio

import (
	"fmt"

	"github.com/resonatehq/resonate/pkg/idempotency"
	"github.com/resonatehq/resonate/pkg/lock"
	"github.com/resonatehq/resonate/pkg/notification"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/schedule"
	"github.com/resonatehq/resonate/pkg/subscription"
	"github.com/resonatehq/resonate/pkg/task"
	"github.com/resonatehq/resonate/pkg/timeout"
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

	// SUBSCRIPTIONS
	ReadSubscription
	ReadSubscriptions
	CreateSubscription
	DeleteSubscription
	DeleteSubscriptions
	TimeoutDeleteSubscriptions

	// NOTIFICATIONS
	ReadNotifications
	CreateNotifications
	UpdateNotification
	DeleteNotification
	TimeoutCreateNotifications

	// TIMEOUTS
	ReadTimeouts
	CreateTimeout
	DeleteTimeout

	// LOCKS
	ReadLock
	AcquireLock
	HeartbeatLocks
	ReleaseLock
	TimeoutLocks

	// TASKS
	CreateTask
	UpdateTask
	ReadTask
	ReadTasks
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

	// SUBSCRIPTIONS
	case ReadSubscription:
		return "ReadSubscription"
	case ReadSubscriptions:
		return "ReadSubscriptions"
	case CreateSubscription:
		return "CreateSubscription"
	case DeleteSubscription:
		return "DeleteSubscription"
	case DeleteSubscriptions:
		return "DeleteSubscriptions"
	case TimeoutDeleteSubscriptions:
		return "TimeoutDeleteSubscriptions"

	// NOTIFICATIONS
	case ReadNotifications:
		return "ReadNotifications"
	case CreateNotifications:
		return "CreateNotifications"
	case UpdateNotification:
		return "UpdateNotification"
	case DeleteNotification:
		return "DeleteNotification"
	case TimeoutCreateNotifications:
		return "TimeoutCreateNotifications"

	// TIMEOUTS
	case ReadTimeouts:
		return "ReadTimeouts"
	case CreateTimeout:
		return "CreateTimeout"
	case DeleteTimeout:
		return "DeleteTimeout"

	// LOCKS
	case ReadLock:
		return "ReadLock"
	case AcquireLock:
		return "AcquireLock"
	case HeartbeatLocks:
		return "HeartbeatLocks"
	case ReleaseLock:
		return "ReleaseLock"
	case TimeoutLocks:
		return "TimeoutLocks"

	// TASKS
	case CreateTask:
		return "CreateTask"
	case UpdateTask:
		return "UpdateTask"
	case ReadTask:
		return "ReadTask"
	case ReadTasks:
		return "ReadTasks"

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

	// SUBSCRIPTIONS
	ReadSubscription           *ReadSubscriptionCommand
	ReadSubscriptions          *ReadSubscriptionsCommand
	CreateSubscription         *CreateSubscriptionCommand
	DeleteSubscription         *DeleteSubscriptionCommand
	DeleteSubscriptions        *DeleteSubscriptionsCommand
	TimeoutDeleteSubscriptions *TimeoutDeleteSubscriptionsCommand

	// NOTIFICATIONS
	ReadNotifications          *ReadNotificationsCommand
	CreateNotifications        *CreateNotificationsCommand
	UpdateNotification         *UpdateNotificationCommand
	DeleteNotification         *DeleteNotificationCommand
	TimeoutCreateNotifications *TimeoutCreateNotificationsCommand

	// TIMEOUTS
	ReadTimeouts  *ReadTimeoutsCommand
	CreateTimeout *CreateTimeoutCommand
	DeleteTimeout *DeleteTimeoutCommand

	// LOCKS
	ReadLock       *ReadLockCommand
	AcquireLock    *AcquireLockCommand
	HeartbeatLocks *HeartbeatLocksCommand
	ReleaseLock    *ReleaseLockCommand
	TimeoutLocks   *TimeoutLocksCommand

	// TASKS
	CreateTask *CreateTaskCommand
	UpdateTask *UpdateTaskCommand
	ReadTask   *ReadTaskCommand
	ReadTasks  *ReadTasksCommand
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

	// SUBSCRIPTIONS
	ReadSubscription           *QuerySubscriptionsResult
	ReadSubscriptions          *QuerySubscriptionsResult
	CreateSubscription         *AlterSubscriptionsResult
	DeleteSubscription         *AlterSubscriptionsResult
	DeleteSubscriptions        *AlterSubscriptionsResult
	TimeoutDeleteSubscriptions *AlterSubscriptionsResult

	// NOTIFICATIONS
	ReadNotifications          *QueryNotificationsResult
	CreateNotifications        *AlterNotificationsResult
	UpdateNotification         *AlterNotificationsResult
	DeleteNotification         *AlterNotificationsResult
	TimeoutCreateNotifications *AlterNotificationsResult

	// TIMEOUTS
	ReadTimeouts  *QueryTimeoutsResult
	CreateTimeout *AlterTimeoutsResult
	DeleteTimeout *AlterTimeoutsResult

	// LOCKS
	ReadLock       *QueryLocksResult
	AcquireLock    *AlterLocksResult
	HeartbeatLocks *AlterLocksResult
	ReleaseLock    *AlterLocksResult
	TimeoutLocks   *AlterLocksResult

	// TASKS
	CreateTask *AlterTasksResult
	UpdateTask *AlterTasksResult
	ReadTask   *QueryTasksResult
	ReadTasks  *QueryTasksResult
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
	Subscriptions  []*CreateSubscriptionCommand
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

// Timeout commands

type ReadTimeoutsCommand struct {
	N int
}

type CreateTimeoutCommand struct {
	Id   string
	Time int64
}

type DeleteTimeoutCommand struct {
	Id string
}

// This timeout promises command takes into account 'regular' and 'timeout' promises.
// Regular promises are those that have their state updated to REJECTED_TIMEDOUT when the timeout is reached.
// Timeout promises are those that have their state updated to RESOLVED when the timeout is reached.
type TimeoutPromisesCommand struct {
	Time int64
}

type TimeoutDeleteSubscriptionsCommand struct {
	Time int64
}

type TimeoutCreateNotificationsCommand struct {
	Time int64
}

// Timeout results

type QueryTimeoutsResult struct {
	RowsReturned int64
	Records      []*timeout.TimeoutRecord
}

type AlterTimeoutsResult struct {
	RowsAffected int64
}

// Subscription commands

type ReadSubscriptionCommand struct {
	Id        string
	PromiseId string
}

type ReadSubscriptionsCommand struct {
	PromiseId string
	Limit     int
	SortId    *int64
}

type CreateSubscriptionCommand struct {
	Id          string
	PromiseId   string
	Url         string
	RetryPolicy *subscription.RetryPolicy
	CreatedOn   int64
}

type DeleteSubscriptionCommand struct {
	Id        string
	PromiseId string
}

type DeleteSubscriptionsCommand struct {
	PromiseId string
}

// Subscription results

type QuerySubscriptionsResult struct {
	RowsReturned int64
	LastSortId   int64
	Records      []*subscription.SubscriptionRecord
}

type AlterSubscriptionsResult struct {
	RowsAffected int64
}

// Notification commands

type ReadNotificationsCommand struct {
	N int
}

type CreateNotificationsCommand struct {
	PromiseId string
	Time      int64
}

type UpdateNotificationCommand struct {
	Id        string
	PromiseId string
	Time      int64
	Attempt   int64
}

type DeleteNotificationCommand struct {
	Id        string
	PromiseId string
}

// Notification results

type QueryNotificationsResult struct {
	RowsReturned int64
	Records      []*notification.NotificationRecord
}

type AlterNotificationsResult struct {
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

type HeartbeatLocksCommand struct {
	ProcessId string
	Time      int64
}

type ReleaseLockCommand struct {
	ResourceId  string
	ExecutionId string
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

// Task commands

type CreateTaskCommand struct {
	Id              string
	Counter         int
	PromiseId       string
	ClaimTimeout    int64
	CompleteTimeout int64
	PromiseTimeout  int64
	CreatedOn       int64
	CompletedOn     int64
	IsCompleted     bool
}

type UpdateTaskCommand struct {
	Id              string
	Counter         int
	ClaimTimeout    int64
	CompleteTimeout int64
	CompletedOn     int64
	IsCompleted     bool
}

type ReadTaskCommand struct {
	Id string
}

type ReadTasksCommand struct {
	IsCompleted bool
	RunTime     int64
}

// Task results

type QueryTasksResult struct {
	RowsReturned int64
	Records      []*task.TaskRecord
}

type AlterTasksResult struct {
	RowsAffected int64
}

package t_aio

import (
	"fmt"

	"github.com/resonatehq/resonate/pkg/notification"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/schedule"
	"github.com/resonatehq/resonate/pkg/subscription"
	"github.com/resonatehq/resonate/pkg/timeout"
)

type StoreKind int

const (
	ReadPromise StoreKind = iota
	SearchPromises
	CreatePromise
	UpdatePromise
	ReadTimeouts
	CreateTimeout
	DeleteTimeout
	ReadSubscription
	ReadSubscriptions
	CreateSubscription
	DeleteSubscription
	DeleteSubscriptions
	ReadNotifications
	CreateNotifications
	UpdateNotification
	DeleteNotification
	TimeoutPromises
	TimeoutDeleteSubscriptions
	TimeoutCreateNotifications
	CreateSchedule
	ReadSchedule
	ReadSchedules
	UpdateSchedule
	DeleteSchedule
)

func (k StoreKind) String() string {
	switch k {
	case ReadPromise:
		return "ReadPromise"
	case SearchPromises:
		return "SearchPromises"
	case CreatePromise:
		return "CreatePromise"
	case UpdatePromise:
		return "UpdatePromise"
	case ReadTimeouts:
		return "ReadTimeouts"
	case CreateTimeout:
		return "CreateTimeout"
	case DeleteTimeout:
		return "DeleteTimeout"
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
	case ReadNotifications:
		return "ReadNotifications"
	case CreateNotifications:
		return "CreateNotifications"
	case UpdateNotification:
		return "UpdateNotification"
	case DeleteNotification:
		return "DeleteNotification"
	case TimeoutPromises:
		return "TimeoutPromises"
	case TimeoutDeleteSubscriptions:
		return "TimeoutDeleteSubscriptions"
	case TimeoutCreateNotifications:
		return "TimeoutCreateNotifications"
	case CreateSchedule:
		return "CreateSchedule"
	case ReadSchedule:
		return "ReadSchedule"
	case ReadSchedules:
		return "ReadSchedules"
	case UpdateSchedule:
		return "UpdateSchedule"
	case DeleteSchedule:
		return "DeleteSchedule"
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
	Kind                       StoreKind
	ReadPromise                *ReadPromiseCommand
	SearchPromises             *SearchPromisesCommand
	CreatePromise              *CreatePromiseCommand
	UpdatePromise              *UpdatePromiseCommand
	ReadSchedule               *ReadScheduleCommand
	ReadSchedules              *ReadSchedulesCommand
	CreateSchedule             *CreateScheduleCommand
	UpdateSchedule             *UpdateScheduleCommand
	DeleteSchedule             *DeleteScheduleCommand
	ReadTimeouts               *ReadTimeoutsCommand
	CreateTimeout              *CreateTimeoutCommand
	DeleteTimeout              *DeleteTimeoutCommand
	ReadSubscription           *ReadSubscriptionCommand
	ReadSubscriptions          *ReadSubscriptionsCommand
	CreateSubscription         *CreateSubscriptionCommand
	DeleteSubscription         *DeleteSubscriptionCommand
	DeleteSubscriptions        *DeleteSubscriptionsCommand
	ReadNotifications          *ReadNotificationsCommand
	CreateNotifications        *CreateNotificationsCommand
	UpdateNotification         *UpdateNotificationCommand
	DeleteNotification         *DeleteNotificationCommand
	TimeoutPromises            *TimeoutPromisesCommand
	TimeoutDeleteSubscriptions *TimeoutDeleteSubscriptionsCommand
	TimeoutCreateNotifications *TimeoutCreateNotificationsCommand
}

func (c *Command) String() string {
	return c.Kind.String()
}

type Result struct {
	Kind                       StoreKind
	ReadPromise                *QueryPromisesResult
	SearchPromises             *QueryPromisesResult
	CreatePromise              *AlterPromisesResult
	UpdatePromise              *AlterPromisesResult
	ReadSchedule               *QuerySchedulesResult
	ReadSchedules              *QuerySchedulesResult
	CreateSchedule             *AlterSchedulesResult
	UpdateSchedule             *AlterSchedulesResult
	DeleteSchedule             *AlterSchedulesResult
	ReadTimeouts               *QueryTimeoutsResult
	CreateTimeout              *AlterTimeoutsResult
	DeleteTimeout              *AlterTimeoutsResult
	ReadSubscription           *QuerySubscriptionsResult
	ReadSubscriptions          *QuerySubscriptionsResult
	CreateSubscription         *AlterSubscriptionsResult
	DeleteSubscription         *AlterSubscriptionsResult
	DeleteSubscriptions        *AlterSubscriptionsResult
	ReadNotifications          *QueryNotificationsResult
	CreateNotifications        *AlterNotificationsResult
	UpdateNotification         *AlterNotificationsResult
	DeleteNotification         *AlterNotificationsResult
	TimeoutPromises            *AlterPromisesResult
	TimeoutDeleteSubscriptions *AlterSubscriptionsResult
	TimeoutCreateNotifications *AlterNotificationsResult
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
	IdempotencyKey *promise.IdempotencyKey
	Subscriptions  []*CreateSubscriptionCommand
	Tags           map[string]string
	CreatedOn      int64
}

type UpdatePromiseCommand struct {
	Id             string
	State          promise.State
	Value          promise.Value
	IdempotencyKey *promise.IdempotencyKey
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

type CreateScheduleCommand struct {
	Id             string
	Desc           string
	Cron           string
	PromiseId      string
	PromiseParam   promise.Value
	PromiseTimeout int64
	LastRunTime    *int64
	NextRunTime    int64
	CreatedOn      int64
	IdempotencyKey *promise.IdempotencyKey
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

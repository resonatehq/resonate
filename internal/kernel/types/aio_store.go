package types

import (
	"github.com/resonatehq/resonate/pkg/notification"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/subscription"
	"github.com/resonatehq/resonate/pkg/timeout"
)

type StoreKind int

const (
	StoreReadPromise StoreKind = iota
	StoreSearchPromises
	StoreCreatePromise
	StoreUpdatePromise
	StoreReadTimeouts
	StoreCreateTimeout
	StoreDeleteTimeout
	StoreReadSubscription
	StoreReadSubscriptions
	StoreCreateSubscription
	StoreDeleteSubscription
	StoreDeleteSubscriptions
	StoreReadNotifications
	StoreCreateNotifications
	StoreUpdateNotification
	StoreDeleteNotification
	StoreTimeoutPromises
	StoreTimeoutDeleteSubscriptions
	StoreTimeoutCreateNotifications
)

type StoreSubmission struct {
	Transaction *Transaction
}

type StoreCompletion struct {
	Results []*Result
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

type Result struct {
	Kind                       StoreKind
	ReadPromise                *QueryPromisesResult
	SearchPromises             *QueryPromisesResult
	CreatePromise              *AlterPromisesResult
	UpdatePromise              *AlterPromisesResult
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

// Promise commands

type ReadPromiseCommand struct {
	Id string
}

type SearchPromisesCommand struct {
	Q      string
	States []promise.State
	Limit  int
	SortId *int64
}

type CreatePromiseCommand struct {
	Id            string
	Timeout       int64
	Param         promise.Value
	Subscriptions []*CreateSubscriptionCommand
	Tags          map[string]string
	CreatedOn     int64
}

type UpdatePromiseCommand struct {
	Id          string
	State       promise.State
	Value       promise.Value
	CompletedOn int64
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
	PromiseIds []string
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

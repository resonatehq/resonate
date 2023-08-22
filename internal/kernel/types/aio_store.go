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
	StoreReadSubscriptions
	StoreCreateSubscription
	StoreDeleteSubscription
	StoreReadNotifications
	StoreCreateNotification
	StoreUpdateNotification
	StoreDeleteNotification
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
	Kind               StoreKind
	ReadPromise        *ReadPromiseCommand
	SearchPromises     *SearchPromisesCommand
	CreatePromise      *CreatePromiseCommand
	UpdatePromise      *UpdatePromiseCommand
	ReadTimeouts       *ReadTimeoutsCommand
	CreateTimeout      *CreateTimeoutCommand
	DeleteTimeout      *DeleteTimeoutCommand
	ReadSubscriptions  *ReadSubscriptionsCommand
	CreateSubscription *CreateSubscriptionCommand
	DeleteSubscription *DeleteSubscriptionCommand
	ReadNotifications  *ReadNotificationsCommand
	CreateNotification *CreateNotificationCommand
	UpdateNotification *UpdateNotificationCommand
	DeleteNotification *DeleteNotificationCommand
}

type Result struct {
	Kind               StoreKind
	ReadPromise        *QueryPromisesResult
	SearchPromises     *QueryPromisesResult
	CreatePromise      *AlterPromisesResult
	UpdatePromise      *AlterPromisesResult
	ReadTimeouts       *QueryTimeoutsResult
	CreateTimeout      *AlterTimeoutsResult
	DeleteTimeout      *AlterTimeoutsResult
	ReadSubscriptions  *QuerySubscriptionsResult
	CreateSubscription *AlterSubscriptionResult
	DeleteSubscription *AlterSubscriptionResult
	ReadNotifications  *QueryNotificationsResult
	CreateNotification *AlterNotificationsResult
	UpdateNotification *AlterNotificationsResult
	DeleteNotification *AlterNotificationsResult
}

type ReadPromiseCommand struct {
	Id string
}

type SearchPromisesCommand struct {
	Q     string
	State promise.State
}

type CreatePromiseCommand struct {
	Id            string
	Timeout       int64
	Param         promise.Value
	Subscriptions []*CreateSubscriptionCommand
}

type UpdatePromiseCommand struct {
	Id    string
	State promise.State
	Value promise.Value
}

type QueryPromisesResult struct {
	RowsReturned int64
	Records      []*promise.PromiseRecord
}

type AlterPromisesResult struct {
	RowsAffected int64
}

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

type QueryTimeoutsResult struct {
	RowsReturned int64
	Records      []*timeout.TimeoutRecord
}

type AlterTimeoutsResult struct {
	RowsAffected int64
}

type ReadSubscriptionsCommand struct {
	PromiseIds []string
}

type CreateSubscriptionCommand struct {
	PromiseId   string
	Url         string
	RetryPolicy *subscription.RetryPolicy
}

type DeleteSubscriptionCommand struct {
	PromiseId string
	Id        int64
}

type QuerySubscriptionsResult struct {
	RowsReturned int64
	Records      []*subscription.SubscriptionRecord
}

type AlterSubscriptionResult struct {
	RowsAffected int64
	LastInsertId int64
}

type ReadNotificationsCommand struct {
	N int
}

type CreateNotificationCommand struct {
	PromiseId   string
	Url         string
	RetryPolicy []byte
	Time        int64
}

type UpdateNotificationCommand struct {
	Id      int64
	Time    int64
	Attempt int64
}

type DeleteNotificationCommand struct {
	Id int64
}

type QueryNotificationsResult struct {
	RowsReturned int64
	Records      []*notification.NotificationRecord
}

type AlterNotificationsResult struct {
	RowsAffected int64
	LastInsertId int64
}

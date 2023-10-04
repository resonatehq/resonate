package t_api

type Kind int

const (
	// Promise
	ReadPromise Kind = iota
	SearchPromises
	CreatePromise
	CancelPromise
	ResolvePromise
	RejectPromise

	// Subscription
	ReadSubscriptions
	CreateSubscription
	DeleteSubscription
)

package types

type APIKind int

const (
	// Promise
	ReadPromise APIKind = iota
	SearchPromises
	CreatePromise
	CancelPromise
	ResolvePromise
	RejectPromise
	CompletePromise

	// Subscription
	ReadSubscriptions
	CreateSubscription
	DeleteSubscription
)

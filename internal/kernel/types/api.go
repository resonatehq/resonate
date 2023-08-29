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

	// Subscription
	ReadSubscriptions
	CreateSubscription
	DeleteSubscription
)

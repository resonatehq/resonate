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

func (k APIKind) String() string {
	switch k {
	case ReadPromise:
		return "ReadPromise"
	case SearchPromises:
		return "SearchPromises"
	case CreatePromise:
		return "CreatePromise"
	case CancelPromise:
		return "CancelPromise"
	case ResolvePromise:
		return "ResolvePromise"
	case RejectPromise:
		return "RejectPromise"
	case ReadSubscriptions:
		return "ReadSubscriptions"
	case CreateSubscription:
		return "CreateSubscription"
	case DeleteSubscription:
		return "DeleteSubscription"
	default:
		panic("invalid request")
	}
}

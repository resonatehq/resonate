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

	// Echo
	Echo
)

func (k Kind) String() string {
	switch k {
	case ReadPromise:
		return "read-promise"
	case SearchPromises:
		return "search-promises"
	case CreatePromise:
		return "create-promise"
	case CancelPromise:
		return "cancel-promise"
	case ResolvePromise:
		return "resolve-promise"
	case RejectPromise:
		return "reject-promise"
	case ReadSubscriptions:
		return "read-subscriptions"
	case CreateSubscription:
		return "create-subscription"
	case DeleteSubscription:
		return "delete-subscription"
	default:
		panic("invalid api")
	}
}

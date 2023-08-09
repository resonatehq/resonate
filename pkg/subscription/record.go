package subscription

type SubscriptionRecord struct {
	PromiseId string
	Id        int64
	Url       string
}

func (r *SubscriptionRecord) Subscription() *Subscription {
	return &Subscription{
		Id:  r.Id,
		Url: r.Url,
	}
}

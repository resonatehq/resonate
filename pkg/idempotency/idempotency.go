package idempotency

type Key string

func (i1 *Key) Match(i2 *Key) bool {
	return i1 != nil && i2 != nil && *i1 == *i2
}

func (i *Key) String() string {
	return string(*i)
}

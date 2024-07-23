package idempotency

type Key string

func (i1 *Key) Match(i2 *Key) bool {
	return i1 != nil && i2 != nil && *i1 == *i2
}

func (i1 *Key) Equals(i2 *Key) bool {
	// for dst only
	return (i1 == nil && i2 == nil) || i1.Match(i2)
}

func (i *Key) String() string {
	return string(*i)
}

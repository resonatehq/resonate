package idempotency

import "strings"

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

func (i *Key) Clone() *Key {
	if i == nil {
		return nil
	}
	k := Key(strings.Clone(i.String()))
	return &k
}

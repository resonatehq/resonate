package metadata

import (
	"fmt"
	"strings"
)

type Metadata struct {
	TransactionId string
	Tags          Tags
}

func New(id string) *Metadata {
	return &Metadata{
		TransactionId: id,
		Tags:          Tags{},
	}
}

func (m Metadata) String() string {
	return fmt.Sprintf("Metadata(tid=%s, tags=%s)", m.TransactionId, m.Tags)
}

type Tags map[string]string

func (t Tags) Set(key string, val string) {
	t[key] = val
}

func (t Tags) Get(key string) string {
	return t[key]
}

func (t Tags) Split(key string) []string {
	return strings.Split(t[key], ",")
}

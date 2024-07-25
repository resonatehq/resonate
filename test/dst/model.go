package dst

import (
	"sort"

	"github.com/resonatehq/resonate/pkg/lock"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/schedule"
)

// Model

type Model struct {
	promises  *Store[*promise.Promise]
	schedules *Store[*schedule.Schedule]
	locks     *Store[*lock.Lock]
}

func NewModel() *Model {
	return &Model{
		promises:  &Store[*promise.Promise]{},
		schedules: &Store[*schedule.Schedule]{},
		locks:     &Store[*lock.Lock]{},
	}
}

func (m *Model) Copy() *Model {
	return &Model{
		promises:  m.promises.copy(),
		schedules: m.schedules.copy(),
		locks:     m.locks.copy(),
	}
}

func (m1 *Model) Equals(m2 *Model) bool {
	return m1.promises.equals(m2.promises) && m1.schedules.equals(m2.schedules) && m1.locks.equals(m2.locks)
}

// Store

type comparable[T any] interface {
	Equals(o T) bool
}

type Store[T comparable[T]] []*struct {
	id    string
	value T
}

func (s *Store[T]) copy() *Store[T] {
	copied := make(Store[T], len(*s))
	copy(copied, *s)
	return &copied
}

func (s *Store[T]) get(id string) T {
	i := sort.Search(len(*s), func(i int) bool {
		return (*s)[i].id >= id
	})

	if i < len(*s) && (*s)[i].id == id {
		return (*s)[i].value
	}

	var zero T
	return zero
}

func (s *Store[T]) set(id string, value T) {
	i := sort.Search(len(*s), func(i int) bool {
		return (*s)[i].id >= id
	})

	if i < len(*s) && (*s)[i].id == id {
		(*s)[i] = &struct {
			id    string
			value T
		}{id: id, value: value}
		return
	}

	*s = append(*s, nil)
	copy((*s)[i+1:], (*s)[i:])
	(*s)[i] = &struct {
		id    string
		value T
	}{id: id, value: value}
}

func (s *Store[T]) delete(id string) {
	i := sort.Search(len(*s), func(i int) bool {
		return (*s)[i].id >= id
	})

	if i < len(*s) && (*s)[i].id == id {
		*s = append((*s)[:i], (*s)[i+1:]...)
	}
}

func (s1 *Store[T]) equals(s2 *Store[T]) bool {
	if s1 == s2 {
		return true
	}

	if len(*s1) != len(*s2) {
		return false
	}

	for i, v1 := range *s1 {
		if !v1.value.Equals((*s2)[i].value) {
			return false
		}
	}

	return true
}

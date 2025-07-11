package dst

import (
	"sort"

	"github.com/resonatehq/resonate/pkg/callback"
	"github.com/resonatehq/resonate/pkg/lock"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/schedule"
	"github.com/resonatehq/resonate/pkg/task"
)

// Model

type Model struct {
	promises  *Store[string, *promise.Promise]
	callbacks *Store[string, *callback.Callback]
	schedules *Store[string, *schedule.Schedule]
	locks     *Store[string, *lock.Lock]
	tasks     *Store[string, *task.Task]
}

func NewModel() *Model {
	return &Model{
		promises:  &Store[string, *promise.Promise]{},
		callbacks: &Store[string, *callback.Callback]{},
		schedules: &Store[string, *schedule.Schedule]{},
		locks:     &Store[string, *lock.Lock]{},
		tasks:     &Store[string, *task.Task]{},
	}
}

func (m *Model) Copy() *Model {
	return &Model{
		promises:  m.promises.copy(),
		callbacks: m.callbacks.copy(),
		schedules: m.schedules.copy(),
		locks:     m.locks.copy(),
		tasks:     m.tasks.copy(),
	}
}

func (m1 *Model) Equals(m2 *Model) bool {
	return m1.promises.equals(m2.promises) &&
		m1.callbacks.equals(m2.callbacks) &&
		m1.schedules.equals(m2.schedules) &&
		m1.locks.equals(m2.locks) &&
		m1.tasks.equals(m2.tasks)
}

// Store

type relatable interface {
	~int | ~int64 | ~string
}

type equatable[T any] interface {
	Equals(o T) bool
}

type Store[I relatable, T equatable[T]] []*struct {
	id    I
	value T
}

func (s *Store[I, T]) copy() *Store[I, T] {
	copied := make(Store[I, T], len(*s))
	copy(copied, *s)
	return &copied
}

func (s *Store[I, T]) all() []T {
	var values []T
	for _, item := range *s {
		values = append(values, item.value)
	}

	return values
}

func (s *Store[I, T]) get(id I) T {
	i := sort.Search(len(*s), func(i int) bool {
		return (*s)[i].id >= id
	})

	if i < len(*s) && (*s)[i].id == id {
		return (*s)[i].value
	}

	var zero T
	return zero
}

func (s *Store[I, T]) set(id I, value T) {
	i := sort.Search(len(*s), func(i int) bool {
		return (*s)[i].id >= id
	})

	if i < len(*s) && (*s)[i].id == id {
		(*s)[i] = &struct {
			id    I
			value T
		}{id: id, value: value}
		return
	}

	*s = append(*s, nil)
	copy((*s)[i+1:], (*s)[i:])
	(*s)[i] = &struct {
		id    I
		value T
	}{id: id, value: value}
}

func (s *Store[I, T]) delete(id I) {
	i := sort.Search(len(*s), func(i int) bool {
		return (*s)[i].id >= id
	})

	if i < len(*s) && (*s)[i].id == id {
		*s = append((*s)[:i], (*s)[i+1:]...)
	}
}

func (s1 *Store[I, T]) equals(s2 *Store[I, T]) bool {
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

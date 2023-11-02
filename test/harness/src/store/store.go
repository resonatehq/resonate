package store

import "sync"

type Store struct {
	mu      sync.Mutex
	history []Operation
}

func NewStore() *Store {
	return &Store{
		mu:      sync.Mutex{},
		history: make([]Operation, 0),
	}
}

func (s *Store) Add(op Operation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.history = append(s.history, op)
}

func (s *Store) History() []Operation {
	return s.history
}

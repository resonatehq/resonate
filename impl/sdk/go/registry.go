package resonate

import "sync"

// Registry maps a task's function name (the value carried in TaskData.Func)
// to a validated, prebuilt durableFunction. Names are explicit — the caller
// passes them at Register time — so they stay stable across renames and
// refactors of the underlying Go function.
//
// Lifecycle: Register all functions at worker startup; Core's dispatch path
// then reads concurrently from many goroutines without contention.
type Registry struct {
	mu    sync.RWMutex
	funcs map[string]*durableFunction
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{funcs: map[string]*durableFunction{}}
}

// Register validates fn and stores it under name. Returns
// *AlreadyRegisteredError if name is already taken, or the underlying
// validation error from durableFunctionFor if fn has an unsupported shape.
func (r *Registry) Register(name string, fn any) error {
	df, err := durableFunctionFor(fn)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.funcs[name]; ok {
		return &AlreadyRegisteredError{Name: name}
	}
	r.funcs[name] = df
	return nil
}

// Get returns the cached durableFunction for name, or (nil, false) if absent.
func (r *Registry) Get(name string) (*durableFunction, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	df, ok := r.funcs[name]
	return df, ok
}

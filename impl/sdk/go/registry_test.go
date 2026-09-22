package resonate

import (
	"errors"
	"testing"
)

func TestRegistry_RegisterAndGet(t *testing.T) {
	r := NewRegistry()
	fn := func(x int) (int, error) { return x * 2, nil }
	if err := r.Register("double", fn); err != nil {
		t.Fatalf("Register: %v", err)
	}
	df, ok := r.Get("double")
	if !ok || df == nil {
		t.Fatalf("Get(double) missing")
	}
}

func TestRegistry_DuplicateRegister(t *testing.T) {
	r := NewRegistry()
	fn := func() (int, error) { return 0, nil }
	if err := r.Register("f", fn); err != nil {
		t.Fatalf("first Register: %v", err)
	}
	err := r.Register("f", fn)
	var dup *AlreadyRegisteredError
	if !errors.As(err, &dup) {
		t.Fatalf("expected *AlreadyRegisteredError, got %T (%v)", err, err)
	}
	if dup.Name != "f" {
		t.Errorf("Name = %q, want %q", dup.Name, "f")
	}
}

func TestRegistry_InvalidSignaturePropagates(t *testing.T) {
	r := NewRegistry()
	// missing error return — durableFunctionFor should reject.
	bad := func(x int) int { return x }
	if err := r.Register("bad", bad); err == nil {
		t.Fatalf("expected validation error, got nil")
	}
	if _, ok := r.Get("bad"); ok {
		t.Errorf("invalid function should not be stored")
	}
}

func TestRegistry_GetMissing(t *testing.T) {
	r := NewRegistry()
	if _, ok := r.Get("nope"); ok {
		t.Errorf("Get on empty registry should be (nil, false)")
	}
}

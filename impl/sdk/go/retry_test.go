package resonate_test

import (
	"errors"
	"testing"
	"time"

	resonate "github.com/resonatehq/resonate-sdk-go"
)

func TestConstantRetry_NextDelay(t *testing.T) {
	p := resonate.ConstantRetry{MaxAttempts: 3, Delay: 50 * time.Millisecond}
	cases := []struct {
		attempt   int
		wantDelay time.Duration
		wantRetry bool
	}{
		{1, 50 * time.Millisecond, true},
		{2, 50 * time.Millisecond, true},
		{3, 0, false},
		{4, 0, false},
	}
	for _, tc := range cases {
		gotDelay, gotRetry := p.NextDelay(tc.attempt, nil)
		if gotDelay != tc.wantDelay || gotRetry != tc.wantRetry {
			t.Errorf("attempt %d: got (%v, %v), want (%v, %v)",
				tc.attempt, gotDelay, gotRetry, tc.wantDelay, tc.wantRetry)
		}
	}
}

func TestLinearRetry_NextDelay(t *testing.T) {
	p := resonate.LinearRetry{MaxAttempts: 4, Base: 10 * time.Millisecond}
	cases := []struct {
		attempt   int
		wantDelay time.Duration
		wantRetry bool
	}{
		{1, 10 * time.Millisecond, true},
		{2, 20 * time.Millisecond, true},
		{3, 30 * time.Millisecond, true},
		{4, 0, false},
	}
	for _, tc := range cases {
		gotDelay, gotRetry := p.NextDelay(tc.attempt, nil)
		if gotDelay != tc.wantDelay || gotRetry != tc.wantRetry {
			t.Errorf("attempt %d: got (%v, %v), want (%v, %v)",
				tc.attempt, gotDelay, gotRetry, tc.wantDelay, tc.wantRetry)
		}
	}
}

func TestExponentialRetry_NextDelay(t *testing.T) {
	p := resonate.ExponentialRetry{MaxAttempts: 5, Base: 10 * time.Millisecond}
	// Without jitter, doubling progression.
	cases := []struct {
		attempt   int
		wantDelay time.Duration
		wantRetry bool
	}{
		{1, 10 * time.Millisecond, true},
		{2, 20 * time.Millisecond, true},
		{3, 40 * time.Millisecond, true},
		{4, 80 * time.Millisecond, true},
		{5, 0, false},
	}
	for _, tc := range cases {
		gotDelay, gotRetry := p.NextDelay(tc.attempt, nil)
		if gotDelay != tc.wantDelay || gotRetry != tc.wantRetry {
			t.Errorf("attempt %d: got (%v, %v), want (%v, %v)",
				tc.attempt, gotDelay, gotRetry, tc.wantDelay, tc.wantRetry)
		}
	}
}

func TestExponentialRetry_Cap(t *testing.T) {
	p := resonate.ExponentialRetry{MaxAttempts: 10, Base: 100 * time.Millisecond, Max: 250 * time.Millisecond}
	for attempt := 1; attempt < 10; attempt++ {
		d, retry := p.NextDelay(attempt, nil)
		if !retry {
			t.Fatalf("attempt %d unexpectedly not retried", attempt)
		}
		if d > 250*time.Millisecond {
			t.Errorf("attempt %d delay %v exceeds cap 250ms", attempt, d)
		}
	}
}

func TestExponentialRetry_JitterRange(t *testing.T) {
	p := resonate.ExponentialRetry{MaxAttempts: 100, Base: 100 * time.Millisecond, Jitter: true}
	// For attempt N (1-indexed, jitterless delay = Base*2^(N-1)) the jittered
	// value should fall in [base, base*1.5).
	for attempt := 1; attempt <= 4; attempt++ {
		base := 100 * time.Millisecond << (attempt - 1)
		upper := base + base/2
		for i := 0; i < 50; i++ {
			d, retry := p.NextDelay(attempt, nil)
			if !retry {
				t.Fatalf("attempt %d: retry=false", attempt)
			}
			if d < base || d >= upper {
				t.Errorf("attempt %d iter %d: delay %v outside [%v, %v)", attempt, i, d, base, upper)
			}
		}
	}
}

func TestNoRetry(t *testing.T) {
	d, retry := resonate.NoRetry.NextDelay(1, errors.New("boom"))
	if retry {
		t.Errorf("NoRetry: expected retry=false on attempt 1, got delay=%v retry=true", d)
	}
}

func TestNewNonRetryable_PreservesError(t *testing.T) {
	sentinel := errors.New("sentinel")
	wrapped := resonate.NewNonRetryable(sentinel)
	if wrapped == nil {
		t.Fatal("NewNonRetryable returned nil for non-nil input")
	}
	if !errors.Is(wrapped, sentinel) {
		t.Errorf("errors.Is(wrapped, sentinel) = false, want true")
	}
	var nr resonate.NonRetryable
	if !errors.As(wrapped, &nr) {
		t.Errorf("errors.As did not see NonRetryable")
	}
	if got := wrapped.Error(); got != "sentinel" {
		t.Errorf("Error() = %q, want %q", got, "sentinel")
	}
	if got := resonate.NewNonRetryable(nil); got != nil {
		t.Errorf("NewNonRetryable(nil) = %v, want nil", got)
	}
}

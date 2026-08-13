package resonate

// Compliance tests for the promise id format the server enforces.
//
// The server (resonatehq/resonate, "new promise id" / PR #1127) treats a
// promise id as `<origin>:<lineage>`: the origin is everything before the
// first ":" and the lineage segments below it are "."-separated:
//
//	root -> root:1 -> root:1.1 -> root:1.1.1
//
// serverValidate is a direct port of the server's
// validate_promise_create_data, and serverOrigin of its origin() helper.
// Every promise the SDK creates is replayed through them here, so a drift in
// id minting fails locally instead of as a 400 from a real server.

import (
	"errors"
	"regexp"
	"strings"
	"testing"
)

// ── The server's rules, ported ──────────────────────────────────────────

// serverOrigin returns the origin, per the server's origin(): text before the
// first ":".
func serverOrigin(id string) string {
	origin, _, _ := strings.Cut(id, ":")
	return origin
}

// serverValidate is a port of the server's validate_promise_create_data.
func serverValidate(t *testing.T, id string, tags map[string]string) {
	t.Helper()
	if strings.ContainsRune(id, '\x00') {
		t.Errorf("null_bytes: id=%q", id)
	}
	if o, ok := tags["resonate:origin"]; ok {
		if strings.Contains(o, ".") {
			t.Errorf("dot_in_origin: origin=%q", o)
		}
		if strings.Contains(o, ":") {
			t.Errorf("colon_in_origin: origin=%q", o)
		}
		if id != o && !strings.HasPrefix(id, o+":") {
			t.Errorf("origin_prefix: id=%q is not prefixed by origin=%q", id, o)
		}
	}
	for _, key := range []string{"resonate:branch", "resonate:parent"} {
		if ancestor, ok := tags[key]; ok {
			// A bare root joins its first lineage segment with ':'; an
			// ancestor that already carries lineage joins deeper ones with '.'.
			sep := ":"
			if strings.Contains(ancestor, ":") {
				sep = "."
			}
			if id != ancestor && !strings.HasPrefix(id, ancestor+sep) {
				t.Errorf("%s_prefix: id=%q is not prefixed by %s=%q", key, id, key, ancestor)
			}
		}
	}
	if p, ok := tags["resonate:prefix"]; ok {
		if strings.Contains(p, ".") {
			t.Errorf("dot_in_prefix: prefix=%q", p)
		}
	}
}

// ── Workflow under test ─────────────────────────────────────────────────

// runIDFormatWorkflow drives a workflow tree through a fake fence client and
// returns every promise record it created: nested ctx.Run children, a durable
// sleep, and a detached child spawned from a nested context.
func runIDFormatWorkflow(t *testing.T) map[string]PromiseRecord {
	t.Helper()
	fake := newFakeFenceClient()
	eff := NewEffects(fake, "task-1", 1, "wf", nil)
	ctx := testContext("wf", eff)

	leaf := func(_ *Context, n int) (int, error) { return n, nil }
	grandchild := func(c *Context, n int) (int, error) {
		f, err := c.Run(leaf, n)
		if err != nil {
			return 0, err
		}
		var out int
		if err := f.Await(&out); err != nil {
			return 0, err
		}
		return out, nil
	}
	mid := func(c *Context, n int) (int, error) {
		f, err := c.Run(grandchild, n)
		if err != nil {
			return 0, err
		}
		var out int
		if err := f.Await(&out); err != nil {
			return 0, err
		}
		// A global-scope timer promise: minted from the same seq as
		// everything else. Not awaited (it would suspend).
		if _, err := c.Sleep(0); err != nil {
			return 0, err
		}
		// Detached from a *nested* context: its id is minted off the origin,
		// not off this context, so its declared ancestors must be the origin
		// too.
		if _, err := c.Detached("tail", n); err != nil {
			return 0, err
		}
		return out, nil
	}

	for _, n := range []int{1, 2} {
		f, err := ctx.Run(mid, n)
		if err != nil {
			t.Fatalf("run mid: %v", err)
		}
		var out int
		if err := f.Await(&out); err != nil {
			t.Fatalf("await mid: %v", err)
		}
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	out := make(map[string]PromiseRecord, len(fake.records))
	for id, rec := range fake.records {
		out[id] = rec
	}
	return out
}

// ── Tests ───────────────────────────────────────────────────────────────

func TestEveryCreatedPromisePassesServerValidation(t *testing.T) {
	promises := runIDFormatWorkflow(t)
	if len(promises) < 5 {
		t.Fatalf("expected a real tree, got %d promises", len(promises))
	}
	for id, rec := range promises {
		serverValidate(t, id, rec.Tags)
	}
}

func TestWholeWorkflowSharesOneOrigin(t *testing.T) {
	// The origin is the server's partition key and the unit both
	// promise.register_callback and task.suspend match on, so every promise a
	// workflow creates — detached children included — must share it.
	promises := runIDFormatWorkflow(t)
	for id, rec := range promises {
		if got := serverOrigin(id); got != "wf" {
			t.Errorf("id %q has origin %q, want wf", id, got)
		}
		if got := rec.Tags["resonate:origin"]; got != "wf" {
			t.Errorf("id %q has resonate:origin %q, want wf", id, got)
		}
	}
}

func TestChildIDsAreColonThenDotSeparated(t *testing.T) {
	promises := runIDFormatWorkflow(t)
	// First level below the root joins with ':', deeper levels with '.'.
	for _, want := range []string{"wf:1", "wf:1.1", "wf:1.1.1"} {
		if _, ok := promises[want]; !ok {
			t.Errorf("expected promise %q, have %v", want, keysOf(promises))
		}
	}
	// No id keeps the old all-'.' shape.
	for id := range promises {
		if strings.HasPrefix(id, "wf.") {
			t.Errorf("id %q keeps the old all-'.' shape", id)
		}
	}
}

func TestDetachedIDsStayBoundedBelowTheOrigin(t *testing.T) {
	// Detached ids are `{origin}:d{16 hex}` — one segment past the origin no
	// matter how deep the spawning context is.
	promises := runIDFormatWorkflow(t)
	detachedRe := regexp.MustCompile(`^wf:d[0-9a-f]{16}$`)
	detached := []string{}
	for id := range promises {
		if strings.HasPrefix(id, "wf:d") {
			detached = append(detached, id)
		}
	}
	if len(detached) != 2 { // one per mid invocation
		t.Fatalf("expected 2 detached children, got %v", detached)
	}
	for _, id := range detached {
		if !detachedRe.MatchString(id) {
			t.Errorf("detached id %q does not match %v", id, detachedRe)
		}
		rec := promises[id]
		if got := rec.Tags["resonate:parent"]; got != "wf" {
			t.Errorf("detached %q resonate:parent = %q, want wf", id, got)
		}
		if got := rec.Tags["resonate:branch"]; got != id {
			t.Errorf("detached %q resonate:branch = %q, want the id itself", id, got)
		}
	}
}

func TestDetachedFromADetachedChildStaysBounded(t *testing.T) {
	// The recursion-bounding case: a detached child that itself detaches
	// mints `{origin}:d{16hex}` off the same fixed origin, not off its own
	// grown id.
	fake := newFakeFenceClient()
	eff := NewEffects(fake, "task-1", 1, "wf", nil)
	detachedCtx := testContext("wf:dabcdefabcdefabcd", eff)
	detachedCtx.originID = "wf" // as seeded from the resonate:origin tag

	id, err := detachedCtx.Detached("tail", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`^wf:d[0-9a-f]{16}$`).MatchString(id) {
		t.Errorf("recursively detached id %q is not one segment past the origin", id)
	}
	rec, ok := fake.record(id)
	if !ok {
		t.Fatalf("promise %q not created", id)
	}
	serverValidate(t, id, rec.Tags)
}

func TestPrefixTagIsNotEmitted(t *testing.T) {
	promises := runIDFormatWorkflow(t)
	for id, rec := range promises {
		if _, ok := rec.Tags["resonate:prefix"]; ok {
			t.Errorf("promise %q emits resonate:prefix", id)
		}
	}
}

func TestJoinIDMatchesTheServersSeparatorRule(t *testing.T) {
	for _, tc := range []struct{ ancestor, segment, want string }{
		{"root", "1", "root:1"},
		{"root:1", "2", "root:1.2"},
		{"root:1.2", "3", "root:1.2.3"},
		{"root", "dbeef", "root:dbeef"},
	} {
		if got := JoinID(tc.ancestor, tc.segment); got != tc.want {
			t.Errorf("JoinID(%q, %q) = %q, want %q", tc.ancestor, tc.segment, got, tc.want)
		}
	}
}

func TestOriginOfMatchesTheServersOrigin(t *testing.T) {
	for _, id := range []string{"root", "root:1", "root:1.2", "root:dbeef"} {
		if got, want := OriginOf(id), serverOrigin(id); got != want {
			t.Errorf("OriginOf(%q) = %q, want %q", id, got, want)
		}
	}
}

func TestValidateRootIDRejectsReservedSeparators(t *testing.T) {
	// Both separators are reserved in a root id: it becomes the origin of its
	// whole lineage, and the server rejects an origin containing either one
	// outright (dot_in_origin / colon_in_origin).
	for _, id := range []string{"a.b", "a:b", "a.b:c", "", "a\x00b"} {
		err := ValidateRootID(id)
		var invalid *InvalidIDError
		if !errors.As(err, &invalid) {
			t.Errorf("ValidateRootID(%q) = %v, want *InvalidIDError", id, err)
		}
	}
}

func TestValidateRootIDAcceptsBareIDs(t *testing.T) {
	for _, id := range []string{"a", "a-b", "a_b", "wf-1786636678653183000"} {
		if err := ValidateRootID(id); err != nil {
			t.Errorf("ValidateRootID(%q) = %v, want nil", id, err)
		}
	}
}

func keysOf(m map[string]PromiseRecord) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

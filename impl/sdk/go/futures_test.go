package resonate

import (
	"errors"
	"strings"
	"testing"
)

func TestFuture_Remote_Await_Resolved(t *testing.T) {
	ctx := testContext("root", nil)
	rec := resolvedPromise(t, "p-1", 42)
	f := &Future{id: "p-1", ctx: ctx, kind: futureRemote, record: &rec}
	var got int
	if err := f.Await(&got); err != nil {
		t.Fatal(err)
	}
	if got != 42 {
		t.Fatalf("expected 42, got %d", got)
	}
}

func TestFuture_Remote_Await_Resolved_DiscardInto(t *testing.T) {
	ctx := testContext("root", nil)
	rec := resolvedPromise(t, "p-1", 42)
	f := &Future{id: "p-1", ctx: ctx, kind: futureRemote, record: &rec}
	if err := f.Await(nil); err != nil {
		t.Fatal(err)
	}
}

func TestFuture_Remote_Await_Rejected(t *testing.T) {
	ctx := testContext("root", nil)
	rec := rejectedPromise(t, "p-1", "boom")
	f := &Future{id: "p-1", ctx: ctx, kind: futureRemote, record: &rec}
	var got int
	err := f.Await(&got)
	if err == nil {
		t.Fatal("expected error")
	}
	var app *ApplicationError
	if !errors.As(err, &app) {
		t.Fatalf("expected *ApplicationError, got %T: %v", err, err)
	}
	if app.Message != "boom" {
		t.Fatalf("expected 'boom', got %q", app.Message)
	}
}

func TestFuture_Remote_Await_Pending_PanicsAndRegistersTodo(t *testing.T) {
	ctx := testContext("root", nil)
	rec := pendingPromise("p-pending")
	f := &Future{id: "p-pending", ctx: ctx, kind: futureRemote, record: &rec}
	assertPanicsWithSuspend(t, func() {
		_ = f.Await(nil)
	})
	todos := ctx.drainSpawnedRemote()
	if len(todos) != 1 || todos[0] != "p-pending" {
		t.Fatalf("expected spawnedRemote=[p-pending], got %v", todos)
	}
}

func TestFuture_Local_Await_PreSettledResolved(t *testing.T) {
	ctx := testContext("root", nil)
	rec := resolvedPromise(t, "loc", "hello")
	f := &Future{id: "loc", ctx: ctx, kind: futureLocal, record: &rec}
	var got string
	if err := f.Await(&got); err != nil {
		t.Fatal(err)
	}
	if got != "hello" {
		t.Fatalf("expected 'hello', got %q", got)
	}
}

func TestFuture_Local_Await_PreSettledRejected(t *testing.T) {
	ctx := testContext("root", nil)
	rec := rejectedPromise(t, "loc", "kaboom")
	f := &Future{id: "loc", ctx: ctx, kind: futureLocal, record: &rec}
	err := f.Await(nil)
	if err == nil || err.Error() != "application error: kaboom" {
		t.Fatalf("expected application error kaboom, got %v", err)
	}
}

func TestFuture_Local_Await_GoroutineCompletes(t *testing.T) {
	ctx := testContext("root", nil)
	ch := make(chan localResult, 1)
	f := &Future{id: "loc", ctx: ctx, kind: futureLocal, result: ch}
	go func() {
		rec := resolvedPromise(t, "loc", 99)
		f.record = &rec
		ch <- localResult{}
	}()
	var got int
	if err := f.Await(&got); err != nil {
		t.Fatal(err)
	}
	if got != 99 {
		t.Fatalf("expected 99, got %d", got)
	}
}

func TestFuture_Local_Await_GoroutineSuspended_RePanics(t *testing.T) {
	ctx := testContext("root", nil)
	ch := make(chan localResult, 1)
	f := &Future{id: "loc", ctx: ctx, kind: futureLocal, result: ch}
	ch <- localResult{suspended: true}
	assertPanicsWithSuspend(t, func() {
		_ = f.Await(nil)
	})
}

func TestFuture_Local_Await_GoroutineError(t *testing.T) {
	ctx := testContext("root", nil)
	ch := make(chan localResult, 1)
	f := &Future{id: "loc", ctx: ctx, kind: futureLocal, result: ch}
	ch <- localResult{err: errors.New("infra: out of memory")}
	err := f.Await(nil)
	if err == nil || !strings.Contains(err.Error(), "out of memory") {
		t.Fatalf("expected infra error, got %v", err)
	}
}

func TestFuture_Local_DoubleAwait_ReturnsSameValue(t *testing.T) {
	ctx := testContext("root", nil)
	rec := resolvedPromise(t, "loc", 1)
	f := &Future{id: "loc", ctx: ctx, kind: futureLocal, record: &rec}
	var v int
	if err := f.Await(&v); err != nil {
		t.Fatal(err)
	}
	if v != 1 {
		t.Fatalf("first Await: expected 1, got %d", v)
	}
	v = 0
	if err := f.Await(&v); err != nil {
		t.Fatalf("second Await returned error: %v", err)
	}
	if v != 1 {
		t.Fatalf("second Await: expected 1, got %d", v)
	}
}

func TestFuture_Local_DoubleAwait_GoroutineReadOnce(t *testing.T) {
	ctx := testContext("root", nil)
	ch := make(chan localResult, 1)
	f := &Future{id: "loc", ctx: ctx, kind: futureLocal, result: ch}
	go func() {
		rec := resolvedPromise(t, "loc", 7)
		f.record = &rec
		ch <- localResult{}
	}()
	var v int
	if err := f.Await(&v); err != nil {
		t.Fatal(err)
	}
	if v != 7 {
		t.Fatalf("first Await: expected 7, got %d", v)
	}
	v = 0
	if err := f.Await(&v); err != nil {
		t.Fatalf("second Await returned error: %v", err)
	}
	if v != 7 {
		t.Fatalf("second Await: expected 7, got %d", v)
	}
}

func TestFuture_Remote_DoubleAwait_Resolved_ReturnsSameValue(t *testing.T) {
	ctx := testContext("root", nil)
	rec := resolvedPromise(t, "p", 3)
	f := &Future{id: "p", ctx: ctx, kind: futureRemote, record: &rec}
	var v int
	if err := f.Await(&v); err != nil {
		t.Fatal(err)
	}
	v = 0
	if err := f.Await(&v); err != nil {
		t.Fatalf("second Await returned error: %v", err)
	}
	if v != 3 {
		t.Fatalf("expected 3, got %d", v)
	}
}

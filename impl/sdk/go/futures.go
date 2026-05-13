package resonate

import (
	"fmt"
	"sync"
)

// suspendSignal is the internal panic value used to unwind a workflow when
// Future.Await would otherwise block. It is recovered at the runWorkflow
// boundary and converted into an Outcome.Suspended — never exposed to user
// code as an error.
type suspendSignal struct{}

func decodeSettled(rec PromiseRecord, into any) error {
	switch rec.State {
	case PromiseStateResolved:
		if into == nil {
			return nil
		}
		return rec.Value.Decode(into)
	case PromiseStateRejected, PromiseStateRejectedCanceled, PromiseStateRejectedTimedout:
		return DeserializeError(rec.Value.DataOrNull())
	default:
		return fmt.Errorf("resonate: future %s has unexpected state %q", rec.ID, rec.State)
	}
}

type futureKind uint8

const (
	futureLocal futureKind = iota
	futureRemote
)

// Future is a handle to a durable promise — either remote (RPC, sleep, latent
// promise) or local (Run-spawned goroutine). The kind is an internal detail;
// callers always use the same Await API.
//
// Await is idempotent on settled futures: once the result is known, repeated
// calls return the same value (or the same sticky error). Calls made while
// the future is still pending panic with the internal suspendSignal{} so the
// workflow runtime can unwind and re-enter later.
type Future struct {
	id   string
	ctx  *Context
	kind futureKind

	record *PromiseRecord   // pre-settled at construction OR filled in by the goroutine
	result chan localResult // local only; nil for remote (and for pre-settled locals)

	once sync.Once
	res  localResult // memoised first channel read
}

func newRemoteFuture(id string, ctx *Context, rec PromiseRecord) *Future {
	return &Future{id: id, ctx: ctx, kind: futureRemote, record: &rec}
}

// ID returns the promise ID.
func (f *Future) ID() string { return f.id }

// Await decodes the future's settled value into `into`. If the underlying
// promise is still pending, Await panics with suspendSignal{} (registering a
// remote todo for remote futures); the workflow runtime recovers this and
// reports Outcome.Suspended. Repeated calls on a done future return the same
// value; repeated calls on a pending future re-panic.
func (f *Future) Await(into any) error {
	switch f.kind {
	case futureRemote:
		if f.record.State == PromiseStatePending {
			f.ctx.appendRemoteTodo(f.id)
			panic(suspendSignal{})
		}
	case futureLocal:
		if f.result != nil {
			f.once.Do(func() { f.res = <-f.result })
			if f.res.suspended {
				panic(suspendSignal{})
			}
			if f.res.err != nil {
				return f.res.err
			}
		}
	}
	if f.record == nil {
		return fmt.Errorf("resonate: future %s completed without a settled record", f.id)
	}
	return decodeSettled(*f.record, into)
}

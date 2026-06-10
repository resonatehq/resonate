package resonate

import (
	stdctx "context"
	"time"
)

// Promises is the direct durable-promise API. It operates on promises outside
// the workflow machinery:
// no resonate:* dispatch tags are set, so a promise created here is never
// picked up by a worker — it is settled explicitly via Resolve, Reject, or
// Cancel (or by anyone holding its ID).
//
// Payloads cross the durability boundary through the instance's Codec
// (JSON → optional encryption → base64). Returned PromiseRecords are already
// decoded: read them with rec.Param.Decode(&out) / rec.Value.Decode(&out).
// Obtain via [Resonate.Promises].
type Promises struct {
	r *Resonate
}

// PromiseCreateOptions controls Promises.Create.
type PromiseCreateOptions struct {
	// Param is an arbitrary Go value codec-encoded into the promise's param
	// field. Nil stores an empty payload.
	Param any

	// Tags are stored verbatim on the promise. Unlike Run/RPC, no resonate:*
	// tags are added — a directly created promise has no dispatch target.
	Tags map[string]string
}

// Get fetches a promise by ID (with the configured prefix applied). Returns
// *ServerError with Code 404 when the promise does not exist.
//
// For rejected promises, the decoded rec.Value.Data holds the standard error
// payload; DeserializeError(rec.Value.Data) converts it to an
// *ApplicationError.
func (p *Promises) Get(ctx stdctx.Context, id string) (PromiseRecord, error) {
	rec, err := p.r.sender.PromiseGet(ctx, p.r.prefixID(id))
	if err != nil {
		return PromiseRecord{}, err
	}
	return p.r.codec.DecodePromise(rec)
}

// Create creates a pending promise. The timeout is relative to now; a value
// <= 0 defaults to DefaultTopLevelTimeout (24h). When the server treats the
// create as an idempotent retry it returns the existing record; otherwise a
// conflicting ID yields *ServerError with Code 409.
func (p *Promises) Create(ctx stdctx.Context, id string, timeout time.Duration, opts ...PromiseCreateOptions) (PromiseRecord, error) {
	opt := firstOpt(opts)
	if timeout <= 0 {
		timeout = DefaultTopLevelTimeout
	}
	param, err := p.r.codec.Encode(opt.Param)
	if err != nil {
		return PromiseRecord{}, err
	}
	tags := opt.Tags
	if tags == nil {
		tags = map[string]string{}
	}
	prefixedID := p.r.prefixID(id)
	rec, err := p.r.sender.PromiseCreate(ctx, prefixedID, PromiseCreateReq{
		ID:        prefixedID,
		TimeoutAt: nowMs() + timeout.Milliseconds(),
		Param:     param,
		Tags:      tags,
	})
	if err != nil {
		return PromiseRecord{}, err
	}
	return p.r.codec.DecodePromise(rec)
}

// Resolve settles a promise as successfully completed with the given value.
// Returns *ServerError with Code 404 when the promise does not exist.
func (p *Promises) Resolve(ctx stdctx.Context, id string, value any) (PromiseRecord, error) {
	return p.settle(ctx, id, SettleStateResolved, value)
}

// Reject settles a promise as failed. When value is an error it is encoded as
// the standard error payload, so Handle.Result and DeserializeError round-trip
// it into an *ApplicationError. Returns *ServerError with Code 404 when the
// promise does not exist.
func (p *Promises) Reject(ctx stdctx.Context, id string, value any) (PromiseRecord, error) {
	return p.settle(ctx, id, SettleStateRejected, value)
}

// Cancel settles a promise as explicitly canceled. Error values are encoded
// like in Reject. Returns *ServerError with Code 404 when the promise does
// not exist.
func (p *Promises) Cancel(ctx stdctx.Context, id string, value any) (PromiseRecord, error) {
	return p.settle(ctx, id, SettleStateRejectedCanceled, value)
}

func (p *Promises) settle(ctx stdctx.Context, id string, state SettleState, value any) (PromiseRecord, error) {
	var v Value
	var err error
	if e, ok := value.(error); ok && state != SettleStateResolved {
		v, err = p.r.codec.Encode(EncodeError(e))
	} else {
		v, err = p.r.codec.Encode(value)
	}
	if err != nil {
		return PromiseRecord{}, err
	}
	rec, err := p.r.sender.PromiseSettle(ctx, PromiseSettleReq{
		ID:    p.r.prefixID(id),
		State: state,
		Value: v,
	})
	if err != nil {
		return PromiseRecord{}, err
	}
	return p.r.codec.DecodePromise(rec)
}

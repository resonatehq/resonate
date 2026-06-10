package resonate_test

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"
	"time"

	resonate "github.com/resonatehq/resonate-sdk-go"
	"github.com/resonatehq/resonate-sdk-go/localnet"
)

type orderInput struct {
	Item  string `json:"item"`
	Count int    `json:"count"`
}

func TestPromisesCreateGetRoundtrip(t *testing.T) {
	r := newLocal(t, localConfig{})
	ctx, cancel := testCtx(t)
	defer cancel()

	before := time.Now().UnixMilli()
	created, err := r.Promises().Create(ctx, "p1", time.Hour, resonate.PromiseCreateOptions{
		Param: orderInput{Item: "book", Count: 3},
		Tags:  map[string]string{"kind": "order"},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created.ID != "p1" {
		t.Errorf("ID = %q, want p1", created.ID)
	}
	if created.State != resonate.PromiseStatePending {
		t.Errorf("State = %q, want pending", created.State)
	}
	wantTimeout := before + time.Hour.Milliseconds()
	if created.TimeoutAt < wantTimeout || created.TimeoutAt > wantTimeout+5000 {
		t.Errorf("TimeoutAt = %d, want ≈ %d", created.TimeoutAt, wantTimeout)
	}

	got, err := r.Promises().Get(ctx, "p1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	var param orderInput
	if err := got.Param.Decode(&param); err != nil {
		t.Fatalf("Param.Decode: %v", err)
	}
	if param != (orderInput{Item: "book", Count: 3}) {
		t.Errorf("param = %+v", param)
	}
	if got.Tags["kind"] != "order" {
		t.Errorf("tags = %v", got.Tags)
	}
}

func TestPromisesCreateZeroTimeoutDefaults(t *testing.T) {
	r := newLocal(t, localConfig{})
	ctx, cancel := testCtx(t)
	defer cancel()

	before := time.Now().UnixMilli()
	rec, err := r.Promises().Create(ctx, "p1", 0)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	want := before + resonate.DefaultTopLevelTimeout.Milliseconds()
	if rec.TimeoutAt < want || rec.TimeoutAt > want+5000 {
		t.Errorf("TimeoutAt = %d, want ≈ %d (24h default)", rec.TimeoutAt, want)
	}
}

func TestPromisesResolveRoundtrip(t *testing.T) {
	r := newLocal(t, localConfig{})
	ctx, cancel := testCtx(t)
	defer cancel()

	if _, err := r.Promises().Create(ctx, "p1", time.Hour); err != nil {
		t.Fatalf("Create: %v", err)
	}
	settled, err := r.Promises().Resolve(ctx, "p1", orderInput{Item: "pen", Count: 1})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if settled.State != resonate.PromiseStateResolved {
		t.Errorf("State = %q, want resolved", settled.State)
	}
	var out orderInput
	if err := settled.Value.Decode(&out); err != nil {
		t.Fatalf("Value.Decode: %v", err)
	}
	if out != (orderInput{Item: "pen", Count: 1}) {
		t.Errorf("value = %+v", out)
	}

	// Interop with the subscription/Handle path.
	h, err := r.Get(ctx, "p1")
	if err != nil {
		t.Fatalf("Get handle: %v", err)
	}
	var fromHandle orderInput
	if err := h.Result(ctx, &fromHandle); err != nil {
		t.Fatalf("Handle.Result: %v", err)
	}
	if fromHandle != out {
		t.Errorf("handle result = %+v, want %+v", fromHandle, out)
	}
}

func TestPromisesRejectYieldsApplicationError(t *testing.T) {
	r := newLocal(t, localConfig{})
	ctx, cancel := testCtx(t)
	defer cancel()

	if _, err := r.Promises().Create(ctx, "p1", time.Hour); err != nil {
		t.Fatalf("Create: %v", err)
	}
	rec, err := r.Promises().Reject(ctx, "p1", errors.New("boom"))
	if err != nil {
		t.Fatalf("Reject: %v", err)
	}
	if rec.State != resonate.PromiseStateRejected {
		t.Errorf("State = %q, want rejected", rec.State)
	}

	h, err := r.Get(ctx, "p1")
	if err != nil {
		t.Fatalf("Get handle: %v", err)
	}
	resErr := h.Result(ctx, nil)
	var ae *resonate.ApplicationError
	if !errors.As(resErr, &ae) {
		t.Fatalf("Result error = %T (%v), want *ApplicationError", resErr, resErr)
	}
	if ae.Message != "boom" {
		t.Errorf("message = %q, want boom", ae.Message)
	}
}

func TestPromisesCancel(t *testing.T) {
	r := newLocal(t, localConfig{})
	ctx, cancel := testCtx(t)
	defer cancel()

	if _, err := r.Promises().Create(ctx, "p1", time.Hour); err != nil {
		t.Fatalf("Create: %v", err)
	}
	rec, err := r.Promises().Cancel(ctx, "p1", "no longer needed")
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if rec.State != resonate.PromiseStateRejectedCanceled {
		t.Errorf("State = %q, want rejected_canceled", rec.State)
	}
}

func TestPromisesGetMissingReturns404(t *testing.T) {
	r := newLocal(t, localConfig{})
	ctx, cancel := testCtx(t)
	defer cancel()

	_, err := r.Promises().Get(ctx, "nope")
	var se *resonate.ServerError
	if !errors.As(err, &se) || se.Code != 404 {
		t.Fatalf("err = %v, want *ServerError{Code: 404}", err)
	}
}

func TestPromisesPrefixApplied(t *testing.T) {
	r := newLocal(t, localConfig{Prefix: "pre"})
	ctx, cancel := testCtx(t)
	defer cancel()

	rec, err := r.Promises().Create(ctx, "x", time.Hour)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if rec.ID != "pre:x" {
		t.Errorf("ID = %q, want pre:x", rec.ID)
	}
	if _, err := r.Sender().PromiseGet(ctx, "pre:x"); err != nil {
		t.Errorf("raw PromiseGet(pre:x): %v", err)
	}
	if _, err := r.Promises().Get(ctx, "x"); err != nil {
		t.Errorf("Promises().Get(x): %v", err)
	}
}

func TestPromisesIdempotentRecreate(t *testing.T) {
	r := newLocal(t, localConfig{})
	ctx, cancel := testCtx(t)
	defer cancel()

	first, err := r.Promises().Create(ctx, "p1", time.Hour, resonate.PromiseCreateOptions{Param: "v1"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	second, err := r.Promises().Create(ctx, "p1", time.Hour, resonate.PromiseCreateOptions{Param: "v2"})
	if err != nil {
		t.Fatalf("re-Create: %v", err)
	}
	var firstParam, secondParam string
	_ = first.Param.Decode(&firstParam)
	_ = second.Param.Decode(&secondParam)
	if secondParam != firstParam {
		t.Errorf("re-create param = %q, want existing record's %q", secondParam, firstParam)
	}
}

// xorEncryptor is a trivial reversible Encryptor for testing the codec path.
type xorEncryptor struct{ key byte }

func (x xorEncryptor) Encrypt(p []byte) ([]byte, error) {
	out := make([]byte, len(p))
	for i, b := range p {
		out[i] = b ^ x.key
	}
	return out, nil
}

func (x xorEncryptor) Decrypt(c []byte) ([]byte, error) { return x.Encrypt(c) }

func TestPromisesEncryptedPayloads(t *testing.T) {
	pid := "default"
	r, err := resonate.New(resonate.Config{
		Network:   localnet.NewLocal("default", &pid),
		Heartbeat: resonate.NoopHeartbeat{},
		TTL:       time.Hour,
		Encryptor: xorEncryptor{key: 0x5a},
	})
	if err != nil {
		t.Fatalf("resonate.New: %v", err)
	}
	t.Cleanup(func() { _ = r.Stop() })
	ctx, cancel := testCtx(t)
	defer cancel()

	if _, err := r.Promises().Create(ctx, "p1", time.Hour, resonate.PromiseCreateOptions{
		Param: orderInput{Item: "secret", Count: 7},
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// The raw wire record holds ciphertext: base64-decoding its param data
	// must not yield valid JSON.
	raw, err := r.Sender().PromiseGet(ctx, "p1")
	if err != nil {
		t.Fatalf("raw PromiseGet: %v", err)
	}
	var b64 string
	if err := json.Unmarshal(raw.Param.Data, &b64); err != nil {
		t.Fatalf("raw param data not a JSON string: %v", err)
	}
	cipher, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		t.Fatalf("raw param data not base64: %v", err)
	}
	if json.Valid(cipher) {
		t.Error("raw param payload is plaintext JSON; expected ciphertext")
	}

	// The Promises API transparently decrypts.
	got, err := r.Promises().Get(ctx, "p1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	var param orderInput
	if err := got.Param.Decode(&param); err != nil {
		t.Fatalf("Param.Decode: %v", err)
	}
	if param != (orderInput{Item: "secret", Count: 7}) {
		t.Errorf("param = %+v", param)
	}
}

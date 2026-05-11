package resonate

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"
)

func newCodec() *Codec { return NewCodec(nil) }

func TestCodecRoundtripInteger(t *testing.T) {
	c := newCodec()
	v, err := c.Encode(42)
	if err != nil {
		t.Fatal(err)
	}
	var out int64
	ok, err := c.Decode(v, &out)
	if err != nil || !ok || out != 42 {
		t.Errorf("got (%v, %v, ok=%v); want (42, nil, ok=true)", out, err, ok)
	}
}

func TestCodecRoundtripString(t *testing.T) {
	c := newCodec()
	v, err := c.Encode("hello")
	if err != nil {
		t.Fatal(err)
	}
	var out string
	ok, _ := c.Decode(v, &out)
	if !ok || out != "hello" {
		t.Errorf("got (%q, ok=%v); want (hello, true)", out, ok)
	}
}

func TestCodecRoundtripBool(t *testing.T) {
	c := newCodec()
	v, _ := c.Encode(true)
	var out bool
	ok, _ := c.Decode(v, &out)
	if !ok || out != true {
		t.Errorf("got (%v, ok=%v); want (true, true)", out, ok)
	}
}

func TestCodecRoundtripObject(t *testing.T) {
	c := newCodec()
	obj := map[string]any{"func": "f", "args": []any{1.0, "two"}}
	v, _ := c.Encode(obj)
	var out map[string]any
	ok, _ := c.Decode(v, &out)
	if !ok {
		t.Fatal("decode failed")
	}
	if out["func"] != "f" {
		t.Errorf("func = %v", out["func"])
	}
}

func TestCodecRoundtripArray(t *testing.T) {
	c := newCodec()
	arr := []int{1, 2, 3}
	v, _ := c.Encode(arr)
	var out []int
	ok, _ := c.Decode(v, &out)
	if !ok || len(out) != 3 || out[2] != 3 {
		t.Errorf("got (%v, ok=%v)", out, ok)
	}
}

func TestCodecDecodeBase64StrEmptyReturnsNotOK(t *testing.T) {
	c := newCodec()
	var out any
	ok, err := c.DecodeBase64("", &out)
	if err != nil || ok {
		t.Errorf("got (ok=%v, err=%v); want (false, nil)", ok, err)
	}
}

func TestCodecDecodeBase64StrRoundtrip(t *testing.T) {
	c := newCodec()
	v, _ := c.Encode(map[string]int{"x": 1})
	// Pull the string out of v.Data (which is JSON-quoted)
	var s string
	if err := json.Unmarshal(v.Data, &s); err != nil {
		t.Fatalf("data not a json string: %v", err)
	}
	var out map[string]int
	ok, err := c.DecodeBase64(s, &out)
	if err != nil || !ok || out["x"] != 1 {
		t.Errorf("got (%v, ok=%v, err=%v)", out, ok, err)
	}
}

func TestCodecEncodeNullProducesEmptyData(t *testing.T) {
	c := newCodec()
	v, _ := c.Encode(nil)
	if string(v.Data) != `""` {
		t.Errorf("data = %s, want \"\"", v.Data)
	}
	var out any
	ok, _ := c.Decode(v, &out)
	if ok {
		t.Errorf("decode of null-encoded value should return ok=false, got out=%v", out)
	}
}

func TestCodecEncodeProducesValidBase64(t *testing.T) {
	c := newCodec()
	v, _ := c.Encode("hello")
	var s string
	if err := json.Unmarshal(v.Data, &s); err != nil {
		t.Fatalf("data not a json string: %v", err)
	}
	if !IsValidBase64(s) {
		t.Errorf("expected valid base64, got %q", s)
	}
	bytes, _ := base64.StdEncoding.DecodeString(s)
	var anyV any
	if err := json.Unmarshal(bytes, &anyV); err != nil {
		t.Errorf("base64 payload is not JSON: %v", err)
	}
}

func TestCodecDecodePromiseDecodesParamAndValue(t *testing.T) {
	c := newCodec()
	paramV, _ := c.Encode(map[string]any{"func": "f"})
	valueV, _ := c.Encode(map[string]any{"result": 42.0})

	pr := PromiseRecord{
		ID:    "test",
		State: PromiseStateResolved,
		Param: paramV,
		Value: valueV,
	}
	decoded, err := c.DecodePromise(pr)
	if err != nil {
		t.Fatal(err)
	}

	var pObj map[string]any
	_ = json.Unmarshal(decoded.Param.Data, &pObj)
	if pObj["func"] != "f" {
		t.Errorf("param.func = %v", pObj["func"])
	}
	var vObj map[string]any
	_ = json.Unmarshal(decoded.Value.Data, &vObj)
	if vObj["result"] != 42.0 {
		t.Errorf("value.result = %v", vObj["result"])
	}
}

func TestCodecDecodeInvalidBase64ReturnsError(t *testing.T) {
	c := newCodec()
	bad := Value{Data: json.RawMessage(`"not-base64!!!"`)}
	var out any
	if _, err := c.Decode(bad, &out); err == nil {
		t.Error("expected error, got nil")
	}
}

func TestEncodeErrorShape(t *testing.T) {
	raw := EncodeError(&ApplicationError{Message: "boom"})
	var obj map[string]any
	_ = json.Unmarshal(raw, &obj)
	if obj["__type"] != "error" {
		t.Errorf("__type = %v", obj["__type"])
	}
	if obj["message"] != "application error: boom" {
		t.Errorf("message = %v", obj["message"])
	}
}

func TestDeserializeErrorRoundtrip(t *testing.T) {
	raw := EncodeError(errors.New("kaboom"))
	err := DeserializeError(raw)
	var ae *ApplicationError
	if !errors.As(err, &ae) {
		t.Fatalf("got %T, want *ApplicationError", err)
	}
	if ae.Message != "kaboom" {
		t.Errorf("message = %q, want kaboom", ae.Message)
	}
}

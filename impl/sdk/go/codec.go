package resonate

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// Encryptor is the swappable encryption hook used by Codec at the durability
// boundary. The default is NoopEncryptor (pass-through).
type Encryptor interface {
	Encrypt(plaintext []byte) ([]byte, error)
	Decrypt(ciphertext []byte) ([]byte, error)
}

// NoopEncryptor is the pass-through Encryptor used by default.
type NoopEncryptor struct{}

func (NoopEncryptor) Encrypt(p []byte) ([]byte, error) { return append([]byte(nil), p...), nil }
func (NoopEncryptor) Decrypt(c []byte) ([]byte, error) { return append([]byte(nil), c...), nil }

// Codec handles encoding and decoding of values at the durability boundary.
//
//	Encode: Go value → JSON → encrypt → base64 → Value{Data: base64-string}
//	Decode: Value{Data: base64-string} → base64 → decrypt → JSON → Go value
type Codec struct {
	Encryptor Encryptor
}

// NewCodec returns a Codec with the given Encryptor, or NoopEncryptor if nil.
func NewCodec(enc Encryptor) *Codec {
	if enc == nil {
		enc = NoopEncryptor{}
	}
	return &Codec{Encryptor: enc}
}

// Encode serializes v through JSON → encrypt → base64 and wraps the result in
// a Value. A JSON null value encodes to Value{Data: "\"\""} (empty string).
func (c *Codec) Encode(v any) (Value, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return Value{}, &EncodingError{Msg: err.Error()}
	}
	if string(raw) == "null" {
		return Value{Data: json.RawMessage(`""`)}, nil
	}
	cipher, err := c.Encryptor.Encrypt(raw)
	if err != nil {
		return Value{}, &EncodingError{Msg: fmt.Sprintf("encrypt: %v", err)}
	}
	b64 := base64.StdEncoding.EncodeToString(cipher)
	quoted, _ := json.Marshal(b64)
	return Value{Data: quoted}, nil
}

// Decode reverses Encode. It returns (true, nil) when out was populated;
// (false, nil) when the Value carries no payload (null or empty string).
func (c *Codec) Decode(v Value, out any) (bool, error) {
	if len(v.Data) == 0 {
		return false, nil
	}
	var s string
	if err := json.Unmarshal(v.Data, &s); err != nil {
		if string(v.Data) == "null" {
			return false, nil
		}
		return false, &DecodingError{Msg: "expected string or null data"}
	}
	return c.DecodeBase64(s, out)
}

// DecodeBase64 decodes a base64 → decrypt → JSON pipeline directly from a
// string, bypassing the Value wrapper. Returns (false, nil) for the empty
// string.
func (c *Codec) DecodeBase64(s string, out any) (bool, error) {
	if s == "" {
		return false, nil
	}
	bytes, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return false, &DecodingError{Msg: fmt.Sprintf("base64: %v", err)}
	}
	plain, err := c.Encryptor.Decrypt(bytes)
	if err != nil {
		return false, &DecodingError{Msg: fmt.Sprintf("decrypt: %v", err)}
	}
	if err := json.Unmarshal(plain, out); err != nil {
		return false, &DecodingError{Msg: fmt.Sprintf("json: %v", err)}
	}
	return true, nil
}

// DecodePromise decodes a PromiseRecord's Param and Value fields in place,
// preserving headers. After this call, Param.Data and Value.Data hold the
// decoded JSON of the durable payload (or a JSON null literal if absent).
func (c *Codec) DecodePromise(p PromiseRecord) (PromiseRecord, error) {
	decoded, err := c.decodeValueData(p.Param)
	if err != nil {
		return PromiseRecord{}, err
	}
	p.Param = Value{Headers: p.Param.Headers, Data: decoded}

	decoded, err = c.decodeValueData(p.Value)
	if err != nil {
		return PromiseRecord{}, err
	}
	p.Value = Value{Headers: p.Value.Headers, Data: decoded}
	return p, nil
}

// DecodePromiseJSON parses raw JSON into a PromiseRecord, then decodes its
// Param and Value fields.
func (c *Codec) DecodePromiseJSON(raw json.RawMessage) (PromiseRecord, error) {
	var pr PromiseRecord
	if err := json.Unmarshal(raw, &pr); err != nil {
		return PromiseRecord{}, &DecodingError{Msg: fmt.Sprintf("invalid promise JSON: %v", err)}
	}
	return c.DecodePromise(pr)
}

func (c *Codec) decodeValueData(v Value) (json.RawMessage, error) {
	var inner any
	ok, err := c.Decode(v, &inner)
	if err != nil {
		return nil, err
	}
	if !ok {
		return json.RawMessage("null"), nil
	}
	out, err := json.Marshal(inner)
	if err != nil {
		return nil, &DecodingError{Msg: fmt.Sprintf("re-encode decoded data: %v", err)}
	}
	return out, nil
}

// IsValidBase64 reports whether s decodes as standard base64.
func IsValidBase64(s string) bool {
	_, err := base64.StdEncoding.DecodeString(s)
	return err == nil
}

// EncodeError builds the standard error payload stored on rejected promises:
//
//	{ "__type": "error", "message": "<err.Error()>" }
func EncodeError(err error) json.RawMessage {
	raw, _ := json.Marshal(map[string]any{
		"__type":  "error",
		"message": err.Error(),
	})
	return raw
}

// DeserializeError reconstructs an error from a rejected promise's value.
// Returns an *ApplicationError carrying the embedded message. Unknown shapes
// are wrapped as "unknown error: <raw>".
func DeserializeError(raw json.RawMessage) error {
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err == nil {
		if msg, ok := obj["message"].(string); ok {
			return &ApplicationError{Message: msg}
		}
	}
	return &ApplicationError{Message: "unknown error: " + string(raw)}
}

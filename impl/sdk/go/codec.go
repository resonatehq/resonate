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

// DecodeValue decodes a single encoded Value (base64 → decrypt), preserving
// headers. After this call, Data holds the decoded JSON of the durable payload
// (or a JSON null literal if absent).
func (c *Codec) DecodeValue(v Value) (Value, error) {
	decoded, err := c.decodeValueData(v)
	if err != nil {
		return Value{}, err
	}
	return Value{Headers: v.Headers, Data: decoded}, nil
}

// DecodePromise decodes a PromiseRecord's Param and Value fields in place,
// preserving headers. After this call, Param.Data and Value.Data hold the
// decoded JSON of the durable payload (or a JSON null literal if absent).
func (c *Codec) DecodePromise(p PromiseRecord) (PromiseRecord, error) {
	param, err := c.DecodeValue(p.Param)
	if err != nil {
		return PromiseRecord{}, err
	}
	p.Param = param

	value, err := c.DecodeValue(p.Value)
	if err != nil {
		return PromiseRecord{}, err
	}
	p.Value = value
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

// decodeValueData runs the encoded payload (base64 → decrypt) and returns the
// resulting plaintext bytes verbatim as a json.RawMessage. We deliberately do
// NOT round-trip through `any`: that would (a) lose int64 precision (the
// JSON-→-any path coerces every number to float64, losing values above 2^53)
// and (b) cost two extra JSON passes per promise on the hot DecodePromise
// path. The plaintext is validated with json.Valid before being returned.
func (c *Codec) decodeValueData(v Value) (json.RawMessage, error) {
	if len(v.Data) == 0 {
		return json.RawMessage("null"), nil
	}
	var s string
	if err := json.Unmarshal(v.Data, &s); err != nil {
		if string(v.Data) == "null" {
			return json.RawMessage("null"), nil
		}
		return nil, &DecodingError{Msg: "expected string or null data"}
	}
	if s == "" {
		return json.RawMessage("null"), nil
	}
	cipher, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, &DecodingError{Msg: fmt.Sprintf("base64: %v", err)}
	}
	plain, err := c.Encryptor.Decrypt(cipher)
	if err != nil {
		return nil, &DecodingError{Msg: fmt.Sprintf("decrypt: %v", err)}
	}
	if !json.Valid(plain) {
		return nil, &DecodingError{Msg: "decoded payload is not valid JSON"}
	}
	return plain, nil
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

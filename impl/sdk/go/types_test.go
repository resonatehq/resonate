package resonate

import (
	"encoding/json"
	"testing"
)

func TestValueRoundTripObjectForm(t *testing.T) {
	raw := []byte(`{"headers":{"k":"v"},"data":"hello"}`)
	var v Value
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if v.Headers["k"] != "v" {
		t.Errorf("headers lost: %+v", v.Headers)
	}
	if string(v.Data) != `"hello"` {
		t.Errorf("data = %s, want \"hello\"", v.Data)
	}
}

func TestValueBareValueFallback(t *testing.T) {
	// A bare non-object JSON value should be wrapped as Data with no headers.
	var v Value
	if err := json.Unmarshal([]byte(`42`), &v); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if v.Headers != nil {
		t.Errorf("headers should be nil, got %+v", v.Headers)
	}
	if string(v.Data) != "42" {
		t.Errorf("data = %s, want 42", v.Data)
	}
}

func TestValueNullProducesEmpty(t *testing.T) {
	var v Value
	if err := json.Unmarshal([]byte(`null`), &v); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if v.Headers != nil || v.Data != nil {
		t.Errorf("null should produce zero value, got %+v", v)
	}
}

func TestPromiseStateSnakeCase(t *testing.T) {
	cases := []struct {
		state PromiseState
		want  string
	}{
		{PromiseStatePending, `"pending"`},
		{PromiseStateRejectedCanceled, `"rejected_canceled"`},
		{PromiseStateRejectedTimedout, `"rejected_timedout"`},
	}
	for _, c := range cases {
		b, err := json.Marshal(c.state)
		if err != nil {
			t.Fatalf("marshal %v: %v", c.state, err)
		}
		if string(b) != c.want {
			t.Errorf("marshal %v = %s, want %s", c.state, b, c.want)
		}
	}
}

func TestTaskStateSnakeCase(t *testing.T) {
	b, _ := json.Marshal(TaskStateAcquired)
	if string(b) != `"acquired"` {
		t.Errorf("TaskStateAcquired marshal = %s, want \"acquired\"", b)
	}
}

func TestPromiseRecordCamelCase(t *testing.T) {
	settled := int64(123)
	pr := PromiseRecord{
		ID:        "p1",
		State:     PromiseStateResolved,
		TimeoutAt: 999,
		CreatedAt: 1,
		SettledAt: &settled,
	}
	b, err := json.Marshal(pr)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(b, &obj); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := obj["timeoutAt"]; !ok {
		t.Errorf("missing camelCase timeoutAt in %s", b)
	}
	if _, ok := obj["settledAt"]; !ok {
		t.Errorf("missing camelCase settledAt in %s", b)
	}
}

func TestTaskRecordResumesIsRaw(t *testing.T) {
	// Server may send resumes as array, number, or bool. Each should round-trip
	// without losing fidelity.
	for _, raw := range []string{`["a","b"]`, `7`, `true`} {
		body := []byte(`{"id":"t1","state":"pending","version":1,"resumes":` + raw + `}`)
		var tr TaskRecord
		if err := json.Unmarshal(body, &tr); err != nil {
			t.Fatalf("unmarshal %s: %v", raw, err)
		}
		if string(tr.Resumes) != raw {
			t.Errorf("resumes = %s, want %s", tr.Resumes, raw)
		}
	}
}

func TestTaskDataValueShape(t *testing.T) {
	v, err := TaskDataValue("greet", map[string]any{"name": "world"})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	var got TaskData
	if err := json.Unmarshal(v.Data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Func != "greet" {
		t.Errorf("func = %q, want greet", got.Func)
	}
	if string(got.Args) != `{"name":"world"}` {
		t.Errorf("args = %s, want {\"name\":\"world\"}", got.Args)
	}
}

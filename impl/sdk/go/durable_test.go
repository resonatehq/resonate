package resonate

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

type addArgs struct {
	X, Y int
}

func workflowAdd(_ *Context, a addArgs) (int, error) { return a.X + a.Y, nil }
func leafDouble(x int) (int, error)                  { return x * 2, nil }
func zeroArgWorkflow(_ *Context) (int, error)        { return 7, nil }
func zeroArgLeaf() (int, error)                      { return 11, nil }
func failingLeaf(int) (int, error)                   { return 0, errors.New("boom") }

func TestDurableFunctionFor_NilFunction(t *testing.T) {
	_, err := durableFunctionFor(nil)
	if err == nil || !strings.Contains(err.Error(), "Run requires a function") {
		t.Fatalf("expected 'Run requires a function', got %v", err)
	}
}

func TestDurableFunctionFor_NotAFunction(t *testing.T) {
	_, err := durableFunctionFor(42)
	if err == nil || !strings.Contains(err.Error(), "Run requires a function") {
		t.Fatalf("expected 'Run requires a function', got %v", err)
	}
}

func TestDurableFunctionFor_WrongReturnArity(t *testing.T) {
	bad := func() {}
	_, err := durableFunctionFor(bad)
	if err == nil || !strings.Contains(err.Error(), "must return (T, error)") {
		t.Fatalf("expected return-arity error, got %v", err)
	}
}

func TestDurableFunctionFor_SecondReturnNotError(t *testing.T) {
	bad := func() (int, int) { return 0, 0 }
	_, err := durableFunctionFor(bad)
	if err == nil || !strings.Contains(err.Error(), "second return must be error") {
		t.Fatalf("expected second-return-must-be-error, got %v", err)
	}
}

func TestDurableFunctionFor_TooManyInputs(t *testing.T) {
	bad := func(_ *Context, _ int, _ int) (int, error) { return 0, nil }
	_, err := durableFunctionFor(bad)
	if err == nil || !strings.Contains(err.Error(), "at most one args parameter") {
		t.Fatalf("expected too-many-inputs error, got %v", err)
	}
}

func TestDurableFunctionFor_TwoParamsFirstNotCtx(t *testing.T) {
	bad := func(_ int, _ int) (int, error) { return 0, nil }
	_, err := durableFunctionFor(bad)
	if err == nil || !strings.Contains(err.Error(), "must have *resonate.Context as the first parameter") {
		t.Fatalf("expected first-must-be-ctx error, got %v", err)
	}
}

func TestDurableFunctionFor_WorkflowShape(t *testing.T) {
	df, err := durableFunctionFor(workflowAdd)
	if err != nil {
		t.Fatal(err)
	}
	if !df.hasCtx {
		t.Fatal("expected hasCtx=true")
	}
	if df.argsType == nil {
		t.Fatal("expected non-nil argsType")
	}
	if !strings.Contains(df.name, "workflowAdd") {
		t.Fatalf("unexpected name %q", df.name)
	}
}

func TestDurableFunctionFor_LeafShape(t *testing.T) {
	df, err := durableFunctionFor(leafDouble)
	if err != nil {
		t.Fatal(err)
	}
	if df.hasCtx {
		t.Fatal("expected hasCtx=false")
	}
	if df.argsType == nil {
		t.Fatal("expected non-nil argsType")
	}
}

func TestDurableFunctionFor_ZeroArgWorkflow(t *testing.T) {
	df, err := durableFunctionFor(zeroArgWorkflow)
	if err != nil {
		t.Fatal(err)
	}
	if !df.hasCtx {
		t.Fatal("expected hasCtx=true")
	}
	if df.argsType != nil {
		t.Fatalf("expected nil argsType, got %v", df.argsType)
	}
}

func TestDurableFunctionFor_ZeroArgLeaf(t *testing.T) {
	df, err := durableFunctionFor(zeroArgLeaf)
	if err != nil {
		t.Fatal(err)
	}
	if df.hasCtx {
		t.Fatal("expected hasCtx=false")
	}
	if df.argsType != nil {
		t.Fatal("expected nil argsType")
	}
}

func TestDurableFunction_Invoke_Workflow(t *testing.T) {
	df, _ := durableFunctionFor(workflowAdd)
	res, err := df.invoke(nil, addArgs{X: 2, Y: 3})
	if err != nil {
		t.Fatal(err)
	}
	if res.(int) != 5 {
		t.Fatalf("expected 5, got %v", res)
	}
}

func TestDurableFunction_Invoke_Leaf(t *testing.T) {
	df, _ := durableFunctionFor(leafDouble)
	res, err := df.invoke(nil, 21)
	if err != nil {
		t.Fatal(err)
	}
	if res.(int) != 42 {
		t.Fatalf("expected 42, got %v", res)
	}
}

func TestDurableFunction_Invoke_ZeroArgWorkflow(t *testing.T) {
	df, _ := durableFunctionFor(zeroArgWorkflow)
	res, err := df.invoke(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.(int) != 7 {
		t.Fatalf("expected 7, got %v", res)
	}
}

func TestDurableFunction_Invoke_ZeroArgLeaf(t *testing.T) {
	df, _ := durableFunctionFor(zeroArgLeaf)
	res, err := df.invoke(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.(int) != 11 {
		t.Fatalf("expected 11, got %v", res)
	}
}

func TestDurableFunction_Invoke_ZeroArgWithNonNilArgs(t *testing.T) {
	df, _ := durableFunctionFor(zeroArgLeaf)
	_, err := df.invoke(nil, 99)
	if err == nil || !strings.Contains(err.Error(), "takes no args parameter") {
		t.Fatalf("expected zero-args-with-args error, got %v", err)
	}
}

func TestDurableFunction_Invoke_WrongArgType(t *testing.T) {
	df, _ := durableFunctionFor(workflowAdd)
	_, err := df.invoke(nil, "wrong-type")
	if err == nil || !strings.Contains(err.Error(), "expects args of type") {
		t.Fatalf("expected wrong-type error, got %v", err)
	}
}

func TestDurableFunction_Invoke_NilArgsForStruct(t *testing.T) {
	df, _ := durableFunctionFor(workflowAdd)
	res, err := df.invoke(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.(int) != 0 {
		t.Fatalf("expected zero-value 0, got %v", res)
	}
}

func TestDurableFunction_Invoke_JSONCoercion(t *testing.T) {
	df, _ := durableFunctionFor(workflowAdd)
	raw := json.RawMessage(`{"X": 4, "Y": 5}`)
	res, err := df.invoke(nil, raw)
	if err != nil {
		t.Fatal(err)
	}
	if res.(int) != 9 {
		t.Fatalf("expected 9, got %v", res)
	}
}

func TestDurableFunction_Invoke_BytesCoercion(t *testing.T) {
	df, _ := durableFunctionFor(workflowAdd)
	res, err := df.invoke(nil, []byte(`{"X": 6, "Y": 7}`))
	if err != nil {
		t.Fatal(err)
	}
	if res.(int) != 13 {
		t.Fatalf("expected 13, got %v", res)
	}
}

func TestDurableFunction_Invoke_PropagatesError(t *testing.T) {
	df, _ := durableFunctionFor(failingLeaf)
	_, err := df.invoke(nil, 0)
	if err == nil || err.Error() != "boom" {
		t.Fatalf("expected 'boom', got %v", err)
	}
}

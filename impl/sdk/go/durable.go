package resonate

import (
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"
)

// durableFunction is the runtime representation of a user function passed
// to ctx.Run. Built fresh on every call — a local reflection pass is cheap
// next to the network round-trip each invocation already costs.
type durableFunction struct {
	name     string
	hasCtx   bool
	argsType reflect.Type
	resType  reflect.Type
	value    reflect.Value
}

var (
	ctxPtrType = reflect.TypeOf((*Context)(nil))
	errorIface = reflect.TypeOf((*error)(nil)).Elem()
)

// durableFunctionFor validates fn and builds its durableFunction.
func durableFunctionFor(fn any) (*durableFunction, error) {
	if fn == nil {
		return nil, fmt.Errorf("resonate: Run requires a function, got nil")
	}
	v := reflect.ValueOf(fn)
	t := v.Type()
	if t.Kind() != reflect.Func {
		return nil, fmt.Errorf("resonate: Run requires a function, got %s", t.Kind())
	}

	name := "unknown"
	if rfn := runtime.FuncForPC(v.Pointer()); rfn != nil {
		name = rfn.Name()
	}

	if t.NumOut() != 2 {
		return nil, fmt.Errorf("resonate: function %s must return (T, error), got %d outputs", name, t.NumOut())
	}
	if !t.Out(1).Implements(errorIface) {
		return nil, fmt.Errorf("resonate: function %s must return (T, error), second return must be error, got %s", name, t.Out(1))
	}

	var hasCtx bool
	var argsType reflect.Type
	switch t.NumIn() {
	case 0:
	case 1:
		if t.In(0) == ctxPtrType {
			hasCtx = true
		} else {
			argsType = t.In(0)
		}
	case 2:
		if t.In(0) != ctxPtrType {
			return nil, fmt.Errorf("resonate: function %s with two parameters must have *resonate.Context as the first parameter", name)
		}
		hasCtx = true
		argsType = t.In(1)
	default:
		return nil, fmt.Errorf("resonate: function %s must take at most one args parameter after the optional *resonate.Context, got %d", name, t.NumIn())
	}

	return &durableFunction{
		name:     name,
		hasCtx:   hasCtx,
		argsType: argsType,
		resType:  t.Out(0),
		value:    v,
	}, nil
}

// invoke dispatches the function based on whether it takes *Context and an
// args value. args is coerced to the declared type either by direct
// reflect.Value assignment or by a JSON round-trip (used on recovery, where
// args arrives as a json.RawMessage from the persisted promise param).
func (d *durableFunction) invoke(ctx *Context, args any) (any, error) {
	if d.argsType == nil && args != nil {
		return nil, fmt.Errorf("resonate: function %s takes no args parameter, but args were provided", d.name)
	}

	var inputs []reflect.Value
	if d.hasCtx {
		inputs = append(inputs, reflect.ValueOf(ctx))
	}
	if d.argsType != nil {
		argsVal, err := d.coerceArgs(args)
		if err != nil {
			return nil, err
		}
		inputs = append(inputs, argsVal)
	}

	outs := d.value.Call(inputs)
	result := outs[0].Interface()
	var errOut error
	if !outs[1].IsNil() {
		errOut = outs[1].Interface().(error)
	}
	return result, errOut
}

func (d *durableFunction) coerceArgs(args any) (reflect.Value, error) {
	if args == nil {
		return reflect.Zero(d.argsType), nil
	}
	v := reflect.ValueOf(args)
	if v.Type().AssignableTo(d.argsType) {
		return v, nil
	}
	var raw []byte
	switch a := args.(type) {
	case json.RawMessage:
		raw = a
	case []byte:
		raw = a
	default:
		return reflect.Value{}, fmt.Errorf("resonate: function %s expects args of type %s, got %T", d.name, d.argsType, args)
	}
	ptr := reflect.New(d.argsType)
	if err := json.Unmarshal(raw, ptr.Interface()); err != nil {
		return reflect.Value{}, &DecodingError{Msg: fmt.Sprintf("decode args for %s: %v", d.name, err)}
	}
	return ptr.Elem(), nil
}

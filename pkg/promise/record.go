package promise

import (
	"encoding/json"
)

type PromiseRecord struct {
	Id           string
	State        State
	ParamHeaders []byte
	ParamIkey    *Ikey
	ParamData    []byte
	ValueHeaders []byte
	ValueIkey    *Ikey
	ValueData    []byte
	Timeout      int64
}

func (r *PromiseRecord) Promise() (*Promise, error) {
	param, err := r.Param()
	if err != nil {
		return nil, err
	}

	value, err := r.Value()
	if err != nil {
		return nil, err
	}

	return &Promise{
		Id:      r.Id,
		State:   r.State,
		Timeout: r.Timeout,
		Param:   param,
		Value:   value,
	}, nil
}

func (r *PromiseRecord) Param() (Value, error) {
	var headers map[string]string

	if r.ParamHeaders != nil {
		if err := json.Unmarshal(r.ParamHeaders, &headers); err != nil {
			return Value{}, err
		}
	}

	return Value{
		Headers: headers,
		Ikey:    r.ParamIkey,
		Data:    r.ParamData,
	}, nil
}

func (r *PromiseRecord) Value() (Value, error) {
	var headers map[string]string

	if r.ValueHeaders != nil {
		if err := json.Unmarshal(r.ValueHeaders, &headers); err != nil {
			return Value{}, err
		}
	}

	return Value{
		Headers: headers,
		Ikey:    r.ValueIkey,
		Data:    r.ValueData,
	}, nil
}

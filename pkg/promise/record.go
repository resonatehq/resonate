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
	CreatedOn    *int64
	CompletedOn  *int64
	Tags         []byte
}

func (r *PromiseRecord) Promise() (*Promise, error) {
	param, err := r.param()
	if err != nil {
		return nil, err
	}

	value, err := r.value()
	if err != nil {
		return nil, err
	}

	tags, err := r.tags()
	if err != nil {
		return nil, err
	}

	return &Promise{
		Id:          r.Id,
		State:       r.State,
		Timeout:     r.Timeout,
		Param:       param,
		Value:       value,
		CreatedOn:   r.CreatedOn,
		CompletedOn: r.CompletedOn,
		Tags:        tags,
	}, nil
}

func (r *PromiseRecord) param() (Value, error) {
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

func (r *PromiseRecord) value() (Value, error) {
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

func (r *PromiseRecord) tags() (map[string]string, error) {
	var tags map[string]string

	if r.Tags != nil {
		if err := json.Unmarshal(r.Tags, &tags); err != nil {
			return nil, err
		}
	}

	return tags, nil
}

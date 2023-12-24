package promise

import (
	"encoding/json"

	"github.com/resonatehq/resonate/pkg/idempotency"
)

type PromiseRecord struct {
	Id                        string
	State                     State
	ParamHeaders              []byte
	ParamData                 []byte
	ValueHeaders              []byte
	ValueData                 []byte
	Timeout                   int64
	IdempotencyKeyForCreate   *idempotency.Key
	IdempotencyKeyForComplete *idempotency.Key
	CreatedOn                 *int64
	CompletedOn               *int64
	Tags                      []byte
	SortId                    int64
}

func (r *PromiseRecord) Promise() (*Promise, error) {
	paramHeaders, err := bytesToMap(r.ParamHeaders)
	if err != nil {
		return nil, err
	}

	valueHeaders, err := bytesToMap(r.ValueHeaders)
	if err != nil {
		return nil, err
	}

	tags, err := bytesToMap(r.Tags)
	if err != nil {
		return nil, err
	}

	return &Promise{
		Id:                        r.Id,
		State:                     r.State,
		Param:                     Value{Headers: paramHeaders, Data: r.ParamData},
		Value:                     Value{Headers: valueHeaders, Data: r.ValueData},
		Timeout:                   r.Timeout,
		IdempotencyKeyForCreate:   r.IdempotencyKeyForCreate,
		IdempotencyKeyForComplete: r.IdempotencyKeyForComplete,
		CreatedOn:                 r.CreatedOn,
		CompletedOn:               r.CompletedOn,
		Tags:                      tags,
		SortId:                    r.SortId,
	}, nil
}

func bytesToMap(b []byte) (map[string]string, error) {
	m := map[string]string{}

	if b != nil {
		if err := json.Unmarshal(b, &m); err != nil {
			return nil, err
		}
	}

	return m, nil
}

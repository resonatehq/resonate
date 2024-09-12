package callback

import (
	"encoding/json"
	"fmt"
)

type Callback struct {
	Id            string          `json:"id"`
	PromiseId     string          `json:"promiseId"`
	RootPromiseId string          `json:"rootPromiseId"`
	Timeout       int64           `json:"timeout"`
	Recv          json.RawMessage `json:"recv"`
	CreatedOn     int64           `json:"createdOn"`
}

func (c *Callback) String() string {
	return fmt.Sprintf(
		"Callback(id=%s, promiseId=%s, rootPromiseId=%s, timeout=%d)",
		c.Id,
		c.PromiseId,
		c.RootPromiseId,
		c.Timeout,
	)
}

func (c1 *Callback) Equals(c2 *Callback) bool {
	// for dst only
	return c1.Id == c2.Id && c1.PromiseId == c2.PromiseId
}

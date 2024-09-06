package callback

import (
	"fmt"

	"github.com/resonatehq/resonate/pkg/receiver"
)

type Callback struct {
	Id        string         `json:"id"`
	PromiseId string         `json:"promiseId"`
	Recv      *receiver.Recv `json:"-"` // unexported
	Message   []byte         `json:"-"` // unexported
	Timeout   int64          `json:"timeout"`
	CreatedOn int64          `json:"createdOn"`
}

func (c *Callback) String() string {
	return fmt.Sprintf(
		"Callback(id=%s, promiseId=%s, recv=%s, timeout=%d)",
		c.Id,
		c.PromiseId,
		c.Recv,
		c.Timeout,
	)
}

func (c1 *Callback) Equals(c2 *Callback) bool {
	// for dst only
	return c1.Id == c2.Id && c1.PromiseId == c2.PromiseId
}

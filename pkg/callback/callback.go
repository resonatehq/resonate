package callback

import (
	"fmt"

	"github.com/resonatehq/resonate/pkg/message"
)

type Callback struct {
	Id        string           `json:"id"`
	PromiseId string           `json:"promiseId"`
	Message   *message.Message `json:"message,omitempty"`
	Timeout   int64            `json:"timeout"`
	CreatedOn int64            `json:"createdOn"`
}

func (c *Callback) String() string {
	return fmt.Sprintf(
		"Callback(id=%s, promiseId=%s, message=%s, timeout=%d)",
		c.Id,
		c.PromiseId,
		c.Message,
		c.Timeout,
	)
}

func (c1 *Callback) Equals(c2 *Callback) bool {
	// for dst only
	return c1.Id == c2.Id && c1.PromiseId == c2.PromiseId
}

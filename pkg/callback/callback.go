package callback

import (
	"fmt"

	"github.com/resonatehq/resonate/pkg/message"
)

type Callback struct {
	Id        int64            `json:"id"`
	PromiseId string           `json:"promiseId"`
	Message   *message.Message `json:"commands,omitempty"`
	CreatedOn int64            `json:"createdOn"`
}

func (c *Callback) String() string {
	return fmt.Sprintf(
		"Callback(id=%d, promiseId=%s, message=%s)",
		c.Id,
		c.PromiseId,
		c.Message,
	)
}

func (c1 *Callback) Equals(c2 *Callback) bool {
	// for dst only
	return c1.Id == c2.Id && c1.PromiseId == c2.PromiseId
}

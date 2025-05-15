package validators

import (
	"errors"

	"github.com/resonatehq/resonate/internal/kernel/t_api"
)

func CreateCallback(r *t_api.Request) error {
	if r.CreateCallback.Mesg.Type == "resume" && r.CreateCallback.PromiseId == r.CreateCallback.Mesg.Root {
		return errors.New("promise and root promise must be different promises")
	}
	return nil
}

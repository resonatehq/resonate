package promises

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/resonatehq/resonate/pkg/client"
	v1 "github.com/resonatehq/resonate/pkg/client/v1"
	"github.com/spf13/cobra"
)

var completePromiseExample = `
# %s a promise
resonate promises %s foo

# %s a promise with data and headers
resonate promises %s foo --data foo --header bar=bar`

func CompletePromiseCmds(c client.Client) []*cobra.Command {
	var (
		data           string
		headers        map[string]string
		idempotencyKey string
		strict         bool
	)

	states := []struct {
		Short string
		Title string
		PastT string
		State v1.CompletePromiseJSONBodyState
	}{
		{"resolve", "Resolve", "Resolved", v1.CompletePromiseJSONBodyStateRESOLVED},
		{"reject", "Reject", "Rejected", v1.CompletePromiseJSONBodyStateREJECTED},
		{"cancel", "Cancel", "Canceled", v1.CompletePromiseJSONBodyStateREJECTEDCANCELED},
	}

	cmds := make([]*cobra.Command, 3)
	for i, s := range states {
		state := s
		cmd := &cobra.Command{
			Use:     fmt.Sprintf("%s <id>", state.Short),
			Short:   fmt.Sprintf("%s promise", state.Title),
			Example: fmt.Sprintf(completePromiseExample, state.Title, state.Short, state.Title, state.Short),
			RunE: func(cmd *cobra.Command, args []string) error {
				if len(args) != 1 {
					return errors.New("must specify an id")
				}

				id := args[0]

				params := &v1.CompletePromiseParams{
					Strict: &strict,
				}

				if cmd.Flag("idempotency-key").Changed {
					params.IdempotencyKey = &idempotencyKey
				}

				body := v1.CompletePromiseJSONRequestBody{
					State: state.State,
					Value: &v1.Value{},
				}

				if cmd.Flag("header").Changed {
					body.Value.Headers = &headers
				}

				if cmd.Flag("data").Changed {
					encoded := base64.StdEncoding.EncodeToString([]byte(data))
					body.Value.Data = &encoded
				}

				res, err := c.V1().CompletePromiseWithResponse(context.TODO(), id, params, body)
				if err != nil {
					return err
				}

				if res.StatusCode() == 201 {
					cmd.Printf("%s promise: %s\n", state.PastT, id)
				} else if res.StatusCode() == 200 {
					cmd.Printf("%s promise: %s (deduplicated)\n", state.PastT, id)
				} else {
					cmd.PrintErrln(res.Status(), string(res.Body))
				}

				return nil
			},
		}

		cmd.Flags().StringToStringVar(&headers, "header", map[string]string{}, "promise value header")
		cmd.Flags().StringVar(&data, "data", "", "promise value data")
		cmd.Flags().StringVar(&idempotencyKey, "idempotency-key", "", "idempotency key")
		cmd.Flags().BoolVar(&strict, "strict", true, "strict mode")

		cmds[i] = cmd
	}

	return cmds
}

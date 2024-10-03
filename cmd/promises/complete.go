package promises

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/resonatehq/resonate/pkg/client"
	"github.com/resonatehq/resonate/pkg/client/openapi"
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
		State openapi.CompletePromiseJSONBodyState
	}{
		{"resolve", "Resolve", "Resolved", openapi.CompletePromiseJSONBodyStateRESOLVED},
		{"reject", "Reject", "Rejected", openapi.CompletePromiseJSONBodyStateREJECTED},
		{"cancel", "Cancel", "Canceled", openapi.CompletePromiseJSONBodyStateREJECTEDCANCELED},
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

				params := &openapi.CompletePromiseParams{
					Strict: &strict,
				}

				if cmd.Flag("idempotency-key").Changed {
					params.IdempotencyKey = &idempotencyKey
				}

				body := openapi.CompletePromiseJSONRequestBody{
					State: state.State,
					Value: &openapi.Value{},
				}

				if cmd.Flag("header").Changed {
					body.Value.Headers = &headers
				}

				if cmd.Flag("data").Changed {
					encoded := base64.StdEncoding.EncodeToString([]byte(data))
					body.Value.Data = &encoded
				}

				resp, err := c.CompletePromiseWithResponse(context.TODO(), id, params, body)
				if err != nil {
					return err
				}

				if resp.StatusCode() == 201 {
					cmd.Printf("%s promise: %s\n", state.PastT, id)
				} else if resp.StatusCode() == 200 {
					cmd.Printf("%s promise: %s (deduplicated)\n", state.PastT, id)
				} else {
					cmd.PrintErrln(resp.Status(), string(resp.Body))
				}

				return nil
			},
		}

		cmd.Flags().StringToStringVarP(&headers, "header", "H", map[string]string{}, "promise value header")
		cmd.Flags().StringVarP(&data, "data", "D", "", "promise value data")
		cmd.Flags().StringVarP(&idempotencyKey, "idempotency-key", "i", "", "idempotency key")
		cmd.Flags().BoolVarP(&strict, "strict", "s", true, "strict mode")

		_ = cmd.MarkFlagRequired("id")
		_ = cmd.MarkFlagRequired("state")

		cmds[i] = cmd
	}

	return cmds
}

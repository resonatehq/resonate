package promises

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/resonatehq/resonate/pkg/client"
	"github.com/resonatehq/resonate/pkg/client/promises"
	"github.com/spf13/cobra"
)

var completePromiseExample = `
# %s a promise
resonate promises %s foo

# %s a promise with data and headers
resonate promises %s foo --data foo --header bar=bar`

func CompletePromiseCmds(c client.ResonateClient) []*cobra.Command {
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
		State promises.PromiseStateComplete
	}{
		{"resolve", "Resolve", "Resolved", promises.PromiseStateCompleteRESOLVED},
		{"reject", "Reject", "Rejected", promises.PromiseStateCompleteREJECTED},
		{"cancel", "Cancel", "Canceled", promises.PromiseStateCompleteREJECTEDCANCELED},
	}

	cmds := make([]*cobra.Command, 3)
	for i, s := range states {
		state := s
		cmd := &cobra.Command{
			Use:     fmt.Sprintf("%s <id>", state.Short),
			Short:   fmt.Sprintf("%s a promise", state.Title),
			Example: fmt.Sprintf(completePromiseExample, state.Title, state.Short, state.Title, state.Short),
			Run: func(cmd *cobra.Command, args []string) {
				if len(args) != 1 {
					cmd.PrintErrln("Must specify promise id")
					return
				}

				id := args[0]

				params := &promises.PatchPromisesIdParams{
					Strict: &strict,
				}

				if cmd.Flag("idempotency-key").Changed {
					params.IdempotencyKey = &idempotencyKey
				}

				body := promises.PatchPromisesIdJSONRequestBody{
					State: state.State,
					Value: &promises.PromiseValue{},
				}

				if cmd.Flag("header").Changed {
					body.Value.Headers = headers
				}

				if cmd.Flag("data").Changed {
					encoded := base64.StdEncoding.EncodeToString([]byte(data))
					body.Value.Data = &encoded
				}

				resp, err := c.PromisesV1Alpha1().PatchPromisesIdWithResponse(context.TODO(), id, params, body)
				if err != nil {
					cmd.PrintErr(err)
					return
				}

				if resp.StatusCode() == 201 {
					cmd.Printf("%s promise: %s\n", state.PastT, id)
				} else if resp.StatusCode() == 200 {
					cmd.Printf("%s promise: %s (deduplicated)\n", state.PastT, id)
				} else {
					cmd.PrintErrln(resp.Status(), string(resp.Body))
				}
			},
		}

		cmd.Flags().StringVarP(&data, "data", "D", "", "Promise value data")
		cmd.Flags().StringToStringVarP(&headers, "header", "H", map[string]string{}, "Promise value header")
		cmd.Flags().StringVarP(&idempotencyKey, "idempotency-key", "i", "", "Idempotency key")
		cmd.Flags().BoolVarP(&strict, "strict", "s", true, "Strict mode")

		_ = cmd.MarkFlagRequired("id")
		_ = cmd.MarkFlagRequired("state")

		cmds[i] = cmd
	}

	return cmds
}

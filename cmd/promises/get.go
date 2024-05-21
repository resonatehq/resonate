package promises

import (
	"context"
	"encoding/json"

	"github.com/resonatehq/resonate/pkg/client"
	"github.com/spf13/cobra"
)

var getPromiseExample = `
# Get a promise
resonate promises foo`

func GetPromiseCmd(c client.ResonateClient) *cobra.Command {
	var (
		output string
	)

	cmd := &cobra.Command{
		Use:     "get <id>",
		Short:   "Get a durable promise",
		Example: getPromiseExample,
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 1 {
				cmd.PrintErrln("Must specify promise id")
				return
			}

			id := args[0]

			resp, err := c.PromisesV1Alpha1().GetPromiseWithResponse(context.Background(), id, nil)
			if err != nil {
				cmd.PrintErr(err)
				return
			}

			if resp.StatusCode() != 200 {
				cmd.PrintErrln(resp.Status(), string(resp.Body))
				return
			}

			if output == "json" {
				promise, err := json.MarshalIndent(resp.JSON200, "", "  ")
				if err != nil {
					cmd.PrintErr(err)
					return
				}

				cmd.Println(string(promise))
				return
			}

			prettyPrintPromise(cmd, resp.JSON200)
		},
	}

	cmd.Flags().StringVarP(&output, "output", "o", "", "Output format, can be one of: json")

	return cmd
}

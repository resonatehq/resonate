package promises

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/resonatehq/resonate/pkg/client"
	"github.com/spf13/cobra"
)

var getPromiseExample = `
# Get a promise
resonate promises foo`

func GetPromiseCmd(c client.Client) *cobra.Command {
	var (
		output string
	)

	cmd := &cobra.Command{
		Use:     "get <id>",
		Short:   "Get promise",
		Example: getPromiseExample,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return errors.New("must specify an id")
			}

			id := args[0]

			resp, err := c.ReadPromiseWithResponse(context.TODO(), id, nil)
			if err != nil {
				return err
			}

			if resp.StatusCode() != 200 {
				cmd.PrintErrln(resp.Status(), string(resp.Body))
				return nil
			}

			if output == "json" {
				promise, err := json.MarshalIndent(resp.JSON200, "", "  ")
				if err != nil {
					return err
				}

				cmd.Println(string(promise))
				return nil
			}

			prettyPrintPromise(cmd, resp.JSON200)
			return nil
		},
	}

	cmd.Flags().StringVarP(&output, "output", "o", "", "output format, can be one of: json")

	return cmd
}

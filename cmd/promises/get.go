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

			client, err := c.V1()
			if err != nil {
				return err
			}

			res, err := client.ReadPromiseWithResponse(context.TODO(), id, nil)
			if err != nil {
				return err
			}

			if res.StatusCode() != 200 {
				cmd.PrintErrln(res.Status(), string(res.Body))
				return nil
			}

			if output == "json" {
				promise, err := json.MarshalIndent(res.JSON200, "", "  ")
				if err != nil {
					return err
				}

				cmd.Println(string(promise))
				return nil
			}

			prettyPrintPromise(cmd, res.JSON200)
			return nil
		},
	}

	cmd.Flags().StringVarP(&output, "output", "o", "", "output format, can be one of: json")

	return cmd
}

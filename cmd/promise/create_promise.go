package promise

import (
	"context"
	"encoding/base64"
	"io"

	"github.com/resonatehq/resonate/cmd/util"
	"github.com/resonatehq/resonate/pkg/client"
	"github.com/resonatehq/resonate/pkg/client/promises"
	"github.com/spf13/cobra"
)

var createPromiseExample = `
# Create a minimal promise 
resonate promise create my-promise --timeout 2524608000000

# Create a promise with a data param 
resonate promise create my-promise --timeout 2524608000000 --data '{"foo": "bar"}'

# Create a promise with a data param and headers 
resonate promise create my-promise --timeout 2524608000000 --data '{"foo": "bar"}' --headers Content-Type=application/json
`

func NewCmdCreatePromise(c client.ResonateClient) *cobra.Command {
	var (
		id           string
		paramData    string
		paramHeaders map[string]string
		tags         map[string]string
		timeout      int64
	)

	cmd := &cobra.Command{
		Use:     "create",
		Short:   "Create a promise resource",
		Example: createPromiseExample,
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 1 {
				util.Write(cmd, cmd.OutOrStdout(), "Error: must specify ID\n")
				return
			}
			id = args[0]

			encoded := base64.StdEncoding.EncodeToString([]byte(paramData))

			body := promises.Promise{
				Id: id,
				Param: &promises.Value{
					Data:    &encoded,
					Headers: &paramHeaders,
				},
				Tags:    &tags,
				Timeout: timeout,
			}

			var params *promises.CreatePromiseParams

			resp, err := c.PromisesV1Alpha1().CreatePromise(context.TODO(), params, body)
			if err != nil {
				util.Write(cmd, cmd.OutOrStdout(), "Error: %s\n", err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != 200 && resp.StatusCode != 201 {
				bs, err := io.ReadAll(resp.Body)
				if err != nil {
					util.Write(cmd, cmd.OutOrStdout(), "Error: %s\n", err)
					return
				}

				util.Write(cmd, cmd.OutOrStdout(), "Error: %s\n", string(bs))
				return
			}

			util.Write(cmd, cmd.OutOrStdout(), "Created promise: %s\n", id)
		},
	}

	cmd.Flags().StringVarP(&paramData, "data", "D", "", "Promise data")
	cmd.Flags().StringToStringVarP(&paramHeaders, "headers", "H", map[string]string{}, "Promise headers")
	cmd.Flags().StringToStringVarP(&tags, "tags", "T", map[string]string{}, "Promise tags")
	cmd.Flags().Int64VarP(&timeout, "timeout", "t", 1, "Timeout for promise")

	_ = cmd.MarkFlagRequired("timeout")

	return cmd
}

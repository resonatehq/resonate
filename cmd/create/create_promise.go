package create

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"

	"github.com/resonatehq/resonate/pkg/client"
	"github.com/resonatehq/resonate/pkg/client/promises"
	"github.com/spf13/cobra"
)

func NewCmdCreatePromise(c client.ResonateClient) *cobra.Command {
	var (
		id           string
		paramData    string
		paramHeaders map[string]string
		tags         map[string]string
		timeout      int64
	)

	cmd := &cobra.Command{
		Use:   "promise",
		Short: "Create a promise resource",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 1 {
				fmt.Println("Error: must specify ID")
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
				fmt.Println("Error:", err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != 200 && resp.StatusCode != 201 {
				bs, err := io.ReadAll(resp.Body)
				if err != nil {
					panic(err)
				}
				fmt.Printf("%s\n", string(bs))
				return
			}

			fmt.Printf("Created schedule: %s\n", id)
		},
	}

	cmd.Flags().StringVarP(&paramData, "data", "D", "", "Data value")
	cmd.Flags().StringToStringVarP(&paramHeaders, "headers", "H", map[string]string{}, "Request headers")
	cmd.Flags().StringToStringVarP(&tags, "tags", "T", map[string]string{}, "Promise tags")
	cmd.Flags().Int64VarP(&timeout, "timeout", "t", 1, "Timeout for promise")

	_ = cmd.MarkFlagRequired("timeout")

	return cmd
}

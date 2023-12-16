package patch

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"

	"github.com/resonatehq/resonate/pkg/client"
	"github.com/resonatehq/resonate/pkg/client/promises"
	"github.com/spf13/cobra"
)

func NewCmdPatchPromise(c client.ResonateClient) *cobra.Command {
	var (
		id, state, paramData string
		paramHeaders         map[string]string
	)

	cmd := &cobra.Command{
		Use:   "promise",
		Short: "Patch a promise resource",
		Long:  "Patch the state and value of a pending promise.",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 1 {
				fmt.Println("Error: must specify ID")
				return
			}
			id = args[0]

			encoded := base64.StdEncoding.EncodeToString([]byte(paramData))

			u := promises.PromiseStateComplete(state)
			body := promises.PromiseCompleteRequest{
				State: &u,
				Value: &promises.Value{
					Data:    &encoded,
					Headers: &paramHeaders,
				},
			}

			var params *promises.PatchPromisesIdParams

			resp, err := c.PromisesV1Alpha1().PatchPromisesId(context.TODO(), id, params, body)
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

			fmt.Printf("Completed promise: %s\n", id)
		},
	}

	cmd.Flags().StringVar(&state, "state", "", "State of the promise")
	cmd.Flags().StringVarP(&paramData, "data", "D", "", "Data value")
	cmd.Flags().StringToStringVarP(&paramHeaders, "headers", "H", map[string]string{}, "Request headers")

	_ = cmd.MarkFlagRequired("id")
	_ = cmd.MarkFlagRequired("state")

	return cmd
}

package complete

import (
	"context"
	"fmt"
	"io"

	"github.com/resonatehq/resonate/pkg/client"
	"github.com/resonatehq/resonate/pkg/client/promises"
	"github.com/spf13/cobra"
)

func NewCmdCompletePromise(c client.ResonateClient) *cobra.Command {
	var id string
	var state string
	var value promises.Value

	cmd := &cobra.Command{
		Use:   "promise",
		Short: "Complete a promise resource",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 1 {
				fmt.Println("Error: must specify ID")
				return
			}
			id = args[0]

			u := promises.PromiseStateComplete(state)
			body := promises.PromiseCompleteRequest{
				State: &u,
				Value: &value,
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

	cmd.Flags().StringVar(&id, "id", "", "ID of the promise")
	cmd.Flags().StringVar(&state, "state", "", "State of the promise")

	_ = cmd.MarkFlagRequired("id")
	_ = cmd.MarkFlagRequired("state")

	return cmd
}

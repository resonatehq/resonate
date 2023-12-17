package describe

import (
	"context"
	"fmt"
	"io"

	"github.com/resonatehq/resonate/pkg/client"
	"github.com/spf13/cobra"
)

var describePromiseExample = `
# Describe an existing promise 
resonate describe promise my-promise
`

func NewCmdDescribePromise(c client.ResonateClient) *cobra.Command {
	var id string

	cmd := &cobra.Command{
		Use:     "promise",
		Short:   "Describe a promise resource",
		Example: describePromiseExample,
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 1 {
				fmt.Println("Error: must specify ID")
				return
			}
			id = args[0]

			resp, err := c.PromisesV1Alpha1().GetPromise(context.TODO(), id)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			defer resp.Body.Close()

			bs, err := io.ReadAll(resp.Body)
			if err != nil {
				panic(err)
			}

			if resp.StatusCode != 200 {
				fmt.Printf("%s\n", string(bs))
				return
			}

			fmt.Printf("%s\n", string(bs))
		},
	}

	return cmd
}

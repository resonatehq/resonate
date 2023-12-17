package get

import (
	"context"
	"fmt"
	"io"

	cmd_util "github.com/resonatehq/resonate/cmd/util"

	"github.com/resonatehq/resonate/pkg/client"
	"github.com/resonatehq/resonate/pkg/client/promises"
	"github.com/spf13/cobra"
)

var getPromiseExample = `
# Get a list of promises 
resonate get promises

# Get a list of promises with a specific state 
resonate get promises --state=RESOLVED 

# Get a list of promises with a fuzzy ID expression 
resonate get promises --id=my-promise-*
`

func NewCmdGetPromise(c client.ResonateClient) *cobra.Command {
	var (
		id     string
		state  string
		limit  int
		cursor string
	)
	cmd := &cobra.Command{
		Use:     "promises",
		Aliases: []string{"promise"},
		Short:   "Get a list of promise resources",
		Example: getPromiseExample,
		Run: func(cmd *cobra.Command, args []string) {
			s := promises.PromiseState(state)

			params := &promises.SearchPromisesParams{
				Filters: &promises.QueryFilters{
					Id:     cmd_util.Unwrap(id),
					State:  cmd_util.Unwrap(s),
					Limit:  cmd_util.Unwrap(limit),
					Cursor: cmd_util.Unwrap(cursor),
				},
			}

			resp, err := c.PromisesV1Alpha1().SearchPromises(context.Background(), params)
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

	cmd.Flags().StringVarP(&id, "id", "i", "", "Fuzzy ID expression of the promise")
	cmd.Flags().StringVarP(&state, "state", "s", "", "State of the promise")
	cmd.Flags().IntVarP(&limit, "limit", "l", 100, "Limit the number of results (default: 100)")
	cmd.Flags().StringVarP(&cursor, "cursor", "c", "", "Cursor to use for pagination")

	return cmd
}

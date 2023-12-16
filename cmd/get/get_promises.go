package get

import (
	"context"
	"fmt"
	"io"

	"github.com/resonatehq/resonate/pkg/client"
	"github.com/resonatehq/resonate/pkg/client/promises"
	"github.com/spf13/cobra"
)

func NewCmdGetPromise(c client.ResonateClient) *cobra.Command {
	var (
		id     string
		state  string
		limit  int
		cursor string
	)
	cmd := &cobra.Command{
		Use:   "promise",
		Short: "Get a list of promise resources",
		Run: func(cmd *cobra.Command, args []string) {
			s := promises.PromiseState(state)

			params := &promises.ListPromisesParams{
				Filters: &promises.QueryFilters{
					Id:     &id,
					State:  &s,
					Limit:  &limit,
					Cursor: &cursor,
				},
			}

			resp, err := c.PromisesV1Alpha1().ListPromises(context.Background(), params)
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

	cmd.Flags().StringVar(&id, "id", "", "Fuzzy ID expression of the promise")
	cmd.Flags().StringVar(&state, "state", "", "State of the promise")
	cmd.Flags().IntVar(&limit, "limit", 0, "Limit the number of results")
	cmd.Flags().StringVar(&cursor, "cursor", "", "Cursor to use for pagination")

	return cmd
}

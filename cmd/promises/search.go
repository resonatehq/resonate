package promises

import (
	"context"
	"encoding/json"

	"github.com/resonatehq/resonate/pkg/client"
	"github.com/resonatehq/resonate/pkg/client/promises"
	"github.com/spf13/cobra"
)

var searchPromiseExample = `
# Search for all promise
resonate promise search "*"

# Search for promises that start with foo
resonate promise search "foo.*"

# Search for all pending promises
resonate promise search "*" --state pending

# Search for all resolved promises
resonate promise search "*" --state resolved

# Search for all rejected promises
resonate promise search "*" --state rejected`

func SearchPromisesCmd(c client.ResonateClient) *cobra.Command {
	var (
		state  string
		tags   map[string]string
		limit  int
		cursor string
		output string
	)

	cmd := &cobra.Command{
		Use:     "search <id>",
		Short:   "Search for durables promises",
		Example: searchPromiseExample,
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 1 {
				cmd.PrintErrln("Must specify a search id")
				return
			}

			params := &promises.SearchPromisesParams{
				Id:    &args[0],
				Limit: &limit,
			}

			if cmd.Flag("state").Changed {
				s := promises.SearchPromisesParamsState(state)
				params.State = &s
			}

			if cmd.Flag("tag").Changed {
				params.Tags = &tags
			}

			if cmd.Flag("cursor").Changed {
				params.Cursor = &cursor
			}

			resp, err := c.PromisesV1Alpha1().SearchPromisesWithResponse(context.Background(), params)
			if err != nil {
				cmd.PrintErr(err)
				return
			}

			if resp.StatusCode() != 200 {
				cmd.PrintErr(string(resp.Body))
				return
			}

			if output == "json" {
				for _, p := range *resp.JSON200.Promises {
					promise, err := json.Marshal(p)
					if err != nil {
						cmd.PrintErr(err)
						continue
					}

					cmd.Println(string(promise))
				}
				return
			}

			prettyPrintPromises(cmd, *resp.JSON200.Promises...)
		},
	}

	cmd.Flags().StringVarP(&state, "state", "s", "", "Promise state")
	cmd.Flags().StringToStringVarP(&tags, "tag", "T", map[string]string{}, "Promise tags")
	cmd.Flags().IntVarP(&limit, "limit", "l", 100, "Number of results per request (default: 100)")
	cmd.Flags().StringVarP(&cursor, "cursor", "c", "", "Pagination cursor")
	cmd.Flags().StringVarP(&output, "output", "o", "", "Output format, can be one of: json")

	return cmd
}

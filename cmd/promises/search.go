package promises

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/resonatehq/resonate/pkg/client"
	"github.com/resonatehq/resonate/pkg/client/openapi"
	"github.com/spf13/cobra"
)

var searchPromiseExample = `
# Search for all promises
resonate promises search "*"

# Search for promises that start with foo
resonate promises search "foo.*"

# Search for all pending promises
resonate promises search "*" --state pending

# Search for all resolved promises
resonate promises search "*" --state resolved

# Search for all rejected promises
resonate promises search "*" --state rejected`

func SearchPromisesCmd(c client.Client) *cobra.Command {
	var (
		state  string
		tags   map[string]string
		limit  int
		cursor string
		output string
	)

	cmd := &cobra.Command{
		Use:     "search <q>",
		Short:   "Search promises",
		Example: searchPromiseExample,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return errors.New("must specify search query")
			}

			id := args[0]

			params := &openapi.SearchPromisesParams{
				Id:    &id,
				Limit: &limit,
			}

			if cmd.Flag("state").Changed {
				s := openapi.SearchPromisesParamsState(state)
				params.State = &s
			}

			if cmd.Flag("tag").Changed {
				params.Tags = &tags
			}

			if cmd.Flag("cursor").Changed {
				params.Cursor = &cursor
			}

			resp, err := c.SearchPromisesWithResponse(context.TODO(), params)
			if err != nil {
				return err
			}

			if resp.StatusCode() != 200 {
				cmd.PrintErrln(resp.Status(), string(resp.Body))
				return nil
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
				return nil
			}

			prettyPrintPromises(cmd, *resp.JSON200.Promises...)
			return nil
		},
	}

	cmd.Flags().StringVarP(&state, "state", "s", "", "promise state, can be one of: pending, resolved, rejected")
	cmd.Flags().StringToStringVarP(&tags, "tag", "T", map[string]string{}, "promise tags")
	cmd.Flags().IntVarP(&limit, "limit", "l", 100, "results per page")
	cmd.Flags().StringVarP(&cursor, "cursor", "c", "", "pagination cursor")
	cmd.Flags().StringVarP(&output, "output", "o", "", "output format, can be one of: json")

	return cmd
}

package schedules

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/resonatehq/resonate/pkg/client"
	v1 "github.com/resonatehq/resonate/pkg/client/v1"
	"github.com/spf13/cobra"
)

var searchSchedulesExample = `
# Search for all schedules
resonate schedules search "*"

# Search for schedules that start with foo
resonate schedules search "foo.*"`

func SearchSchedulesCmd(c client.Client) *cobra.Command {
	var (
		tags   map[string]string
		limit  int
		cursor string
		output string
	)

	cmd := &cobra.Command{
		Use:     "search <q>",
		Short:   "Search schedules",
		Example: searchSchedulesExample,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return errors.New("must specify an id")
			}

			id := args[0]

			params := &v1.SearchSchedulesParams{
				Id:    &id,
				Limit: &limit,
			}

			if cmd.Flag("tag").Changed {
				params.Tags = &tags
			}

			if cmd.Flag("cursor").Changed {
				params.Cursor = &cursor
			}

			res, err := c.V1().SearchSchedulesWithResponse(context.Background(), params)
			if err != nil {
				return err
			}

			if res.StatusCode() != 200 {
				cmd.PrintErrln(res.Status(), string(res.Body))
				return nil
			}

			if output == "json" {
				for _, s := range *res.JSON200.Schedules {
					schedule, err := json.Marshal(s)
					if err != nil {
						cmd.PrintErr(err)
						continue
					}

					cmd.Println(string(schedule))
				}
				return nil
			}

			prettyPrintSchedules(cmd, *res.JSON200.Schedules...)
			return nil
		},
	}

	cmd.Flags().StringToStringVar(&tags, "tag", map[string]string{}, "schedule tags")
	cmd.Flags().IntVar(&limit, "limit", 100, "results per page")
	cmd.Flags().StringVar(&cursor, "cursor", "", "pagination cursor")
	cmd.Flags().StringVarP(&output, "output", "o", "", "output format, can be one of: json")

	return cmd
}

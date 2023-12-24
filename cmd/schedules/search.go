package schedules

import (
	"context"
	"encoding/json"

	"github.com/resonatehq/resonate/pkg/client"
	"github.com/resonatehq/resonate/pkg/client/schedules"
	"github.com/spf13/cobra"
)

var searchSchedulesExample = `
# Search for all schedules
resonate schedules search "*"

# Search for schedules that start with foo
resonate schedules search "foo.*"`

func SearchSchedulesCmd(c client.ResonateClient) *cobra.Command {
	var (
		tags   map[string]string
		limit  int
		cursor string
		output string
	)

	cmd := &cobra.Command{
		Use:     "search <id>",
		Short:   "Search for durables schedules",
		Example: searchSchedulesExample,
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 1 {
				cmd.PrintErrln("Must specify a search id")
				return
			}

			params := &schedules.SearchSchedulesParams{
				Id:    &args[0],
				Limit: &limit,
			}

			if cmd.Flag("tag").Changed {
				params.Tags = &tags
			}

			if cmd.Flag("cursor").Changed {
				params.Cursor = &cursor
			}

			resp, err := c.SchedulesV1Alpha1().SearchSchedulesWithResponse(context.Background(), params)
			if err != nil {
				cmd.PrintErr(err)
				return
			}

			if resp.StatusCode() != 200 {
				cmd.PrintErr(string(resp.Body))
				return
			}

			if output == "json" {
				for _, s := range *resp.JSON200.Schedules {
					schedule, err := json.Marshal(s)
					if err != nil {
						cmd.PrintErr(err)
						continue
					}

					cmd.Println(string(schedule))
				}
				return
			}

			prettyPrintSchedules(cmd, *resp.JSON200.Schedules...)
		},
	}

	cmd.Flags().StringToStringVarP(&tags, "tag", "T", map[string]string{}, "Schedule tags")
	cmd.Flags().IntVarP(&limit, "limit", "l", 100, "Number of results per request (default: 100)")
	cmd.Flags().StringVarP(&cursor, "cursor", "c", "", "Pagination cursor")
	cmd.Flags().StringVarP(&output, "output", "o", "", "Output format, can be one of: json")

	return cmd
}

package schedules

import (
	"context"
	"encoding/json"

	"github.com/resonatehq/resonate/pkg/client"
	"github.com/spf13/cobra"
)

var getScheduleExample = `
# Get a schedule
resonate schedules get foo`

func GetScheduleCmd(c client.ResonateClient) *cobra.Command {
	var (
		id     string
		output string
	)

	cmd := &cobra.Command{
		Use:     "get <id>",
		Short:   "Get a schedule",
		Example: getScheduleExample,
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 1 {
				cmd.PrintErrln("Must specify schedule id")
				return
			}

			id = args[0]

			resp, err := c.SchedulesV1Alpha1().GetSchedulesIdWithResponse(context.TODO(), id, nil)
			if err != nil {
				cmd.PrintErr(err)
				return
			}

			if resp.StatusCode() != 200 {
				cmd.PrintErrln(resp.Status(), string(resp.Body))
				return
			}

			if output == "json" {
				schedule, err := json.MarshalIndent(resp.JSON200, "", "  ")
				if err != nil {
					cmd.PrintErr(err)
					return
				}

				cmd.Println(string(schedule))
				return
			}

			prettyPrintSchedule(cmd, resp.JSON200)
		},
	}

	cmd.Flags().StringVarP(&output, "output", "o", "", "Output format, can be one of: json")

	return cmd
}

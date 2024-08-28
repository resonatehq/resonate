package schedules

import (
	"context"
	"fmt"

	"github.com/resonatehq/resonate/pkg/client"
	"github.com/spf13/cobra"
)

var deleteScheduleExample = `
# Delete a schedule
resonate schedules delete foo`

func DeleteScheduleCmd(c client.ResonateClient) *cobra.Command {
	var id string

	cmd := &cobra.Command{
		Use:     "delete <id>",
		Short:   "Delete a schedule",
		Example: deleteScheduleExample,
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 1 {
				cmd.PrintErrln("Must specify schedule id")
				return
			}

			id = args[0]

			resp, err := c.SchedulesV1Alpha1().DeleteSchedulesIdWithResponse(context.TODO(), id, nil)
			if err != nil {
				cmd.PrintErr(err)
				return
			}

			if resp.StatusCode() != 204 {
				cmd.PrintErrln(resp.Status(), string(resp.Body))
				return
			}

			fmt.Println("Deleted schedule:", id)
		},
	}

	return cmd
}

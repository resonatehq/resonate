package schedules

import (
	"context"
	"errors"

	"github.com/resonatehq/resonate/pkg/client"
	"github.com/spf13/cobra"
)

var deleteScheduleExample = `
# Delete a schedule
resonate schedules delete foo`

func DeleteScheduleCmd(c client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "delete <id>",
		Short:   "Delete schedule",
		Example: deleteScheduleExample,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return errors.New("must specify an id")
			}

			id := args[0]

			resp, err := c.DeleteScheduleWithResponse(context.TODO(), id, nil)
			if err != nil {
				return err
			}

			if resp.StatusCode() != 204 {
				cmd.PrintErrln(resp.Status(), string(resp.Body))
				return nil
			}

			cmd.Println("Deleted schedule:", id)
			return nil
		},
	}

	return cmd
}

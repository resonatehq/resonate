package schedules

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/resonatehq/resonate/pkg/client"
	"github.com/spf13/cobra"
)

var getScheduleExample = `
# Get a schedule
resonate schedules get foo`

func GetScheduleCmd(c client.Client) *cobra.Command {
	var (
		output string
	)

	cmd := &cobra.Command{
		Use:     "get <id>",
		Short:   "Get schedule",
		Example: getScheduleExample,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return errors.New("must specify an id")
			}

			id := args[0]

			res, err := c.V1().ReadScheduleWithResponse(context.TODO(), id, nil)
			if err != nil {
				return err
			}

			if res.StatusCode() != 200 {
				cmd.PrintErrln(res.Status(), string(res.Body))
				return nil
			}

			if output == "json" {
				schedule, err := json.MarshalIndent(res.JSON200, "", "  ")
				if err != nil {
					return err
				}

				cmd.Println(string(schedule))
				return nil
			}

			prettyPrintSchedule(cmd, res.JSON200)
			return nil
		},
	}

	cmd.Flags().StringVarP(&output, "output", "o", "", "output format, can be one of: json")

	return cmd
}

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

			client, err := c.V1()
			if err != nil {
				return err
			}

			resp, err := client.ReadScheduleWithResponse(context.TODO(), id, nil)
			if err != nil {
				return err
			}

			if resp.StatusCode() != 200 {
				cmd.PrintErrln(resp.Status(), string(resp.Body))
				return nil
			}

			if output == "json" {
				schedule, err := json.MarshalIndent(resp.JSON200, "", "  ")
				if err != nil {
					return err
				}

				cmd.Println(string(schedule))
				return nil
			}

			prettyPrintSchedule(cmd, resp.JSON200)
			return nil
		},
	}

	cmd.Flags().StringVarP(&output, "output", "o", "", "output format, can be one of: json")

	return cmd
}

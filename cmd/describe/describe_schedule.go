package describe

import (
	"context"
	"fmt"
	"io"

	"github.com/resonatehq/resonate/pkg/client"
	"github.com/spf13/cobra"
)

var describeScheduleExample = `
# Describe an existing schedule 
resonate describe schedule my-schedule
`

func NewCmdDescribeSchedule(c client.ResonateClient) *cobra.Command {
	var id string

	cmd := &cobra.Command{
		Use:     "schedule",
		Short:   "Describe a schedule resource",
		Example: describeScheduleExample,
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 1 {
				fmt.Println("Error: must specify ID")
				return
			}
			id = args[0]

			resp, err := c.SchedulesV1Alpha1().GetSchedulesId(context.TODO(), id)
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

	return cmd
}

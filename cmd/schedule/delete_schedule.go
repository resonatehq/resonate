package schedule

import (
	"context"
	"fmt"
	"io"

	"github.com/resonatehq/resonate/pkg/client"
	"github.com/spf13/cobra"
)

var deleteScheduleExample = `
# Delete a schedule 
resonate schedule delete my-schedule
`

func NewCmdDeleteSchedule(c client.ResonateClient) *cobra.Command {
	var id string

	cmd := &cobra.Command{
		Use:     "delete",
		Short:   "Delete a schedule resource",
		Example: deleteScheduleExample,
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 1 {
				fmt.Println("Error: must specify ID")
				return
			}
			id = args[0]

			resp, err := c.SchedulesV1Alpha1().DeleteSchedulesId(context.TODO(), id)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != 204 {
				bs, err := io.ReadAll(resp.Body)
				if err != nil {
					panic(err)
				}
				fmt.Printf("Error: %s\n", string(bs))
				return
			}

			fmt.Println("Deleted schedule:", id)
		},
	}

	return cmd
}

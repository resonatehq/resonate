package schedules

import (
	"fmt"
	"text/tabwriter"

	"github.com/resonatehq/resonate/cmd/util"
	"github.com/resonatehq/resonate/pkg/client"
	"github.com/resonatehq/resonate/pkg/client/schedules"
	"github.com/spf13/cobra"
)

func NewCmd(c client.ResonateClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "schedules",
		Aliases: []string{"schedule"},
		Short:   "Manage durable schedules",
		Run: func(cmd *cobra.Command, args []string) {
			_ = cmd.Help()
		},
	}

	// Add subcommands
	cmd.AddCommand(GetScheduleCmd(c))
	cmd.AddCommand(CreateScheduleCmd(c))
	cmd.AddCommand(DeleteScheduleCmd(c))

	return cmd
}

func prettyPrintSchedule(cmd *cobra.Command, schedule *schedules.Schedule) {
	w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)

	fmt.Fprintf(w, "Id:\t%v\n", schedule.Id)
	fmt.Fprintf(w, "Desc:\t%s\n", schedule.Desc)
	fmt.Fprintf(w, "Cron:\t%s\n", schedule.Cron)
	fmt.Fprintf(w, "Last run time:\t%s\n", util.SafeDerefToString(schedule.LastRunTime))
	fmt.Fprintf(w, "Next run time:\t%s\n", util.SafeDerefToString(schedule.NextRunTime))

	w.Flush()
}

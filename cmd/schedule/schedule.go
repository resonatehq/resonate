package schedule

import (
	"github.com/resonatehq/resonate/pkg/client"
	"github.com/spf13/cobra"
)

func NewCmd(c client.ResonateClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "schedule",
		Aliases: []string{"schedules"},
		Short:   "Manage a schedule resource",
		Run:     func(cmd *cobra.Command, args []string) {},
	}

	// Add subcommands
	cmd.AddCommand(NewCmdCreateSchedule(c))
	cmd.AddCommand(NewCmdDescribeSchedule(c))
	cmd.AddCommand(NewCmdDeleteSchedule(c))

	return cmd
}

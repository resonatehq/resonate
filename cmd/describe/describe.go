package describe

import (
	"github.com/resonatehq/resonate/pkg/client"
	"github.com/spf13/cobra"
)

func NewCmdDescribe(c client.ResonateClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "describe",
		Short: "Describe a resource from stdin",
		Run:   func(cmd *cobra.Command, args []string) {},
	}

	// Add subcommands
	cmd.AddCommand(NewCmdDescribePromise(c))
	cmd.AddCommand(NewCmdDescribeSchedule(c))

	return cmd
}

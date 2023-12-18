package promise

import (
	"github.com/resonatehq/resonate/pkg/client"
	"github.com/spf13/cobra"
)

func NewCmd(c client.ResonateClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "promise",
		Aliases: []string{"promises"},
		Short:   "Manage a promise resource",
		Run:     func(cmd *cobra.Command, args []string) {},
	}

	// Add subcommands
	cmd.AddCommand(NewCmdCreatePromise(c))
	cmd.AddCommand(NewCmdDescribePromise(c))
	cmd.AddCommand(NewCmdListPromises(c))
	cmd.AddCommand(NewCmdCompletePromise(c))

	return cmd
}

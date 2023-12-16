package create

import (
	"github.com/resonatehq/resonate/pkg/client"
	"github.com/spf13/cobra"
)

func NewCmdCreate(c client.ResonateClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a resource from stdin",
		Run:   func(cmd *cobra.Command, args []string) {},
	}

	// Add subcommands
	cmd.AddCommand(NewCmdCreatePromise(c))
	cmd.AddCommand(NewCmdCreateSchedule(c))

	return cmd
}

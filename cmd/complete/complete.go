package complete

import (
	"github.com/resonatehq/resonate/pkg/client"
	"github.com/spf13/cobra"
)

func NewCmdComplete(c client.ResonateClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "complete",
		Short: "Complete a resource from stdin",
		Run:   func(cmd *cobra.Command, args []string) {},
	}

	// Add subcommands
	cmd.AddCommand(NewCmdCompletePromise(c))

	return cmd
}

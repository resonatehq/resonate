package get

import (
	"github.com/resonatehq/resonate/pkg/client"
	"github.com/spf13/cobra"
)

func NewCmdGet(c client.ResonateClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get",
		Short: "Get a resource from stdin",
		Run:   func(cmd *cobra.Command, args []string) {},
	}

	// Add subcommands
	cmd.AddCommand(NewCmdGetPromise(c))

	return cmd
}

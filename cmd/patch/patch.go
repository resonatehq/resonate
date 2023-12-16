package patch

import (
	"github.com/resonatehq/resonate/pkg/client"
	"github.com/spf13/cobra"
)

func NewCmdPatch(c client.ResonateClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "patch",
		Short: "Patch a resource from stdin",
		Run:   func(cmd *cobra.Command, args []string) {},
	}

	// Add subcommands
	cmd.AddCommand(NewCmdPatchPromise(c))

	return cmd
}

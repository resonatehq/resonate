package version

import (
	"fmt"

	"github.com/resonatehq/resonate/internal/version"
	"github.com/spf13/cobra"
)

func NewCmd() *cobra.Command {
	cmd := &cobra.Command{
		Args:  cobra.NoArgs,
		Use:   "version",
		Short: "Print the version for resonate",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("resonate version %s\n", version.Full())
		},
	}
	return cmd
}

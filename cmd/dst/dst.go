package dst

import (
	"github.com/spf13/cobra"
)

func NewCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dst",
		Short: "Deterministic simulation testing",
		Run: func(cmd *cobra.Command, args []string) {
			_ = cmd.Help()
		},
	}

	// Add subcommands
	cmd.AddCommand(RunDSTCmd())
	cmd.AddCommand(CreateDSTIssueCmd())

	return cmd
}

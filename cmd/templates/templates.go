package templates

import (
	"github.com/spf13/cobra"
)

func NewCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "templates",
		Aliases: []string{"templates"},
		Short:   "Resonate templates",
		Run: func(cmd *cobra.Command, args []string) {
			_ = cmd.Help()
		},
	}

	// Add subcommands
	cmd.AddCommand(ListTemplateCmd())   // list avaiable templates
	cmd.AddCommand(CreateTemplateCmd()) // create a template project

	return cmd
}

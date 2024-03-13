package quickstart

import (
	"github.com/spf13/cobra"
)

func NewCmd() *cobra.Command {
	var quickstartCmd = &cobra.Command{
		Use:   "quickstart [sdk]",
		Short: "Create a minimal getting started app for the specified SDK",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			sdk := args[0]
			printInstructionsFor(sdk, sdk, cmd)
			return nil
		},
	}
	return quickstartCmd
}

func printInstructionsFor(language, alias string, cmd *cobra.Command) {
	cmd.Printf("Getting started with Resonate for %s\n", alias)
	cmd.Println("> 🏴‍☠️ 1. Run git clone git@github.com:resonatehq/" + language + "-quickstart.git quickstart && cd quickstart")
	cmd.Println("> 🏴‍☠️ 2. Follow the Getting Started instructions in Readme.md")
}

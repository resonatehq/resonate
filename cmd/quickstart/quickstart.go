package quickstart

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

func NewCmd() *cobra.Command {
	var quickstartcmd = cobra.Command{
		Use:   "quickstart [sdk]",
		Short: "Create a minimal getting started app for the specified SDK",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			sdk := args[0]
			switch strings.ToLower(sdk) {
			case "ts", "typescript":
				printInstructionsForTypeScript()
			default:
				fmt.Printf("Error: Unknown SDK '%s'.\n", sdk)
			}
		},
	}
	return &quickstartcmd
}

func printInstructionsForTypeScript() {
	fmt.Println("Getting started with Resonate for TypeScript")
	fmt.Println("> 🏴‍☠️ 1. Run git clone git@github.com:resonatehq/ts-quickstart.git quickstart && cd quickstart")
	fmt.Println("> 🏴‍☠️ 2. Follow the Getting Started instructions in Readme.md")
}

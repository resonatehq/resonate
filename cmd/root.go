package cmd

import (
	"os"

	"github.com/resonatehq/resonate/cmd/dev"
	"github.com/resonatehq/resonate/cmd/dst"
	"github.com/resonatehq/resonate/cmd/invoke"
	"github.com/resonatehq/resonate/cmd/migrate"
	"github.com/resonatehq/resonate/cmd/projects"
	"github.com/resonatehq/resonate/cmd/promises"
	"github.com/resonatehq/resonate/cmd/schedules"
	"github.com/resonatehq/resonate/cmd/serve"
	"github.com/resonatehq/resonate/cmd/tasks"
	"github.com/resonatehq/resonate/internal"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:     "resonate",
	Short:   "Resonate: distributed async await",
	Version: internal.Version(),
}

func init() {
	// Add Subcommands
	rootCmd.AddCommand(dev.NewCmd())
	rootCmd.AddCommand(dst.NewCmd())
	rootCmd.AddCommand(invoke.NewCmd())
	rootCmd.AddCommand(migrate.NewCmd())
	rootCmd.AddCommand(projects.NewCmd())
	rootCmd.AddCommand(promises.NewCmd())
	rootCmd.AddCommand(schedules.NewCmd())
	rootCmd.AddCommand(serve.NewCmd())
	rootCmd.AddCommand(tasks.NewCmd())

	// Set default output
	rootCmd.SetOut(os.Stdout)
	rootCmd.SetErr(os.Stderr)
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

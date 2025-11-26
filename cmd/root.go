package cmd

import (
	"os"

	"github.com/resonatehq/resonate/cmd/config"
	"github.com/resonatehq/resonate/cmd/dev"
	"github.com/resonatehq/resonate/cmd/dst"
	"github.com/resonatehq/resonate/cmd/invoke"
	"github.com/resonatehq/resonate/cmd/migrate"
	"github.com/resonatehq/resonate/cmd/projects"
	"github.com/resonatehq/resonate/cmd/promises"
	"github.com/resonatehq/resonate/cmd/schedules"
	"github.com/resonatehq/resonate/cmd/serve"
	"github.com/resonatehq/resonate/cmd/tasks"
	"github.com/resonatehq/resonate/cmd/tree"
	"github.com/resonatehq/resonate/internal"
	httpPlugin "github.com/resonatehq/resonate/internal/app/plugins/http"
	"github.com/resonatehq/resonate/internal/app/plugins/poll"
	"github.com/resonatehq/resonate/internal/app/plugins/sqs"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/router"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/sender"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/postgres"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/sqlite"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/grpc"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/http"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var rootCmd = &cobra.Command{
	Use:     "resonate",
	Short:   "Resonate: distributed async await",
	Version: internal.Version(),
}

func init() {
	// Create config
	cfg := &config.Config{}
	vip := viper.New()

	// Add Subsystems
	cfg.API.Subsystems.Add("http", true, &http.Config{})
	cfg.API.Subsystems.Add("grpc", true, &grpc.Config{})
	cfg.AIO.Subsystems.Add("router", true, &router.Config{})
	cfg.AIO.Subsystems.Add("sender", true, &sender.Config{})
	cfg.AIO.Subsystems.Add("store-sqlite", true, &sqlite.Config{})
	cfg.AIO.Subsystems.Add("store-postgres", false, &postgres.Config{})

	// Add Plugins
	cfg.AIO.Plugins.Add("http", true, &httpPlugin.Config{})
	cfg.AIO.Plugins.Add("poll", true, &poll.Config{})
	cfg.AIO.Plugins.Add("sqs", true, &sqs.Config{})

	// Add Subcommands
	rootCmd.AddCommand(dev.NewCmd(cfg, vip))
	rootCmd.AddCommand(dst.NewCmd())
	rootCmd.AddCommand(invoke.NewCmd())
	rootCmd.AddCommand(migrate.NewCmd(cfg, vip))
	rootCmd.AddCommand(projects.NewCmd())
	rootCmd.AddCommand(promises.NewCmd())
	rootCmd.AddCommand(schedules.NewCmd())
	rootCmd.AddCommand(serve.NewCmd(cfg, vip))
	rootCmd.AddCommand(tasks.NewCmd())
	rootCmd.AddCommand(tree.NewCmd())

	// Set default output
	rootCmd.SetOut(os.Stdout)
	rootCmd.SetErr(os.Stderr)
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

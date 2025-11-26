package dst

import (
	"github.com/resonatehq/resonate/cmd/config"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/router"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/sender"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/postgres"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/sqlite"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func NewCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dst",
		Short: "Deterministic simulation testing",
		Run: func(cmd *cobra.Command, args []string) {
			_ = cmd.Help()
		},
	}

	// Create dst config
	cfg := &config.Config{}
	vip := viper.New()

	// Add dst subsystems
	cfg.AIO.Subsystems.Add("sender", true, &sender.ConfigDST{})
	cfg.AIO.Subsystems.Add("router", true, &router.Config{})
	cfg.AIO.Subsystems.Add("store-sqlite", true, &sqlite.Config{})
	cfg.AIO.Subsystems.Add("store-postgres", false, &postgres.Config{})

	// Add subcommands
	cmd.AddCommand(RunDSTCmd(cfg, vip))
	cmd.AddCommand(CreateDSTIssueCmd())

	return cmd
}

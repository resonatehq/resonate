package dev

import (
	"github.com/resonatehq/resonate/cmd/config"
	"github.com/resonatehq/resonate/cmd/serve"
	"github.com/spf13/cobra"
)

func DevCmd() *cobra.Command {
	var (
		config = &config.Config{}
	)

	cmd := &cobra.Command{
		Use:   "dev",
		Short: "Start Resonate server in development mode",
		Long:  "Start Resonate server with development-friendly defaults (in-memory SQLite store).\n\nThis command is an alias for: resonate serve --aio-store-sqlite-path :memory:",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := config.Parse(); err != nil {
				return err
			}

			// Override dev-specific settings
			config.AIO.Subsystems.StoreSqlite.Config.Path = ":memory:"

			return serve.RunServe(config)
		},
	}

	// bind config (same as serve command)
	_ = config.Bind(cmd)

	// maintain defined order of flags
	cmd.Flags().SortFlags = false

	return cmd
}

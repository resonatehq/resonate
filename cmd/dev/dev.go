package dev

import (
	"github.com/resonatehq/resonate/cmd/config"
	"github.com/resonatehq/resonate/cmd/serve"
	"github.com/spf13/cobra"
)

func NewCmd() *cobra.Command {
	var (
		config = &config.Config{}
	)

	cmd := &cobra.Command{
		Use:   "dev",
		Short: "Start Resonate server in development mode",
		Long:  "Start Resonate server with development-friendly defaults (in-memory SQLite store).\n\nThis command is an alias for: resonate serve --aio-store-sqlite-path ':memory:' --aio-store-postgres-query 'sslmode=disable'\n",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := config.ParseDev(); err != nil {
				return err
			}

			return serve.Serve(config)
		},
	}

	_ = config.BindDev(cmd)
	cmd.Flags().SortFlags = false

	return cmd
}

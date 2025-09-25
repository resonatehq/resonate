package dev

import (
	"github.com/spf13/cobra"
)

func NewCmd(serveCmd *cobra.Command) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dev",
		Short: "Start Resonate server in development mode",
		Long:  "Start Resonate server with development-friendly defaults (in-memory SQLite store).\n\nThis command is an alias for: resonate serve --aio-store-sqlite-path ':memory:' --aio-store-postgres-query 'sslmode=disable'\n",
		RunE:  serveCmd.RunE,
	}

	cmd.Flags().SortFlags = false
	cmd.Flags().AddFlagSet(serveCmd.Flags())

	f := cmd.Flags().Lookup("aio-store-sqlite-path")
	if f != nil {
		f.DefValue = ":memory:"
		_ = cmd.Flags().Set("aio-store-sqlite-path", ":memory:")
	}

	f = cmd.Flags().Lookup("aio-store-postgres-query")
	if f != nil {
		f.DefValue = "sslmode=disable"
		_ = cmd.Flags().Set("aio-store-postgres-query", "sslmode=disable")
	}

	return cmd
}

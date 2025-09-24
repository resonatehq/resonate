package dev

import (
	"github.com/spf13/cobra"
)

func NewCmd(serveCmd *cobra.Command) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dev",
		Short: "Start Resonate server in development mode",
		Long:  "Start Resonate server with development-friendly defaults (in-memory SQLite store).\n\nThis command is an alias for: resonate serve --aio-store-sqlite-path :memory:",
		RunE: func(cmd *cobra.Command, args []string) error {
			if !cmd.Flags().Changed("aio-store-sqlite-path") {
				cmd.Flags().Set("aio-store-sqlite-path", ":memory:")
			}
			return serveCmd.RunE(cmd, args)
		},
	}
	cmd.Flags().SortFlags = false
	cmd.Flags().AddFlagSet(serveCmd.Flags())
	return cmd
}

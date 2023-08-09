package cmd

import (
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "resonate",
	Short: "Durable promises and executions",
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		panic(err)
	}
}

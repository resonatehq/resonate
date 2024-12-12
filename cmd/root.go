package cmd

import (
	"log/slog"
	"os"
	"strings"

	"github.com/resonatehq/resonate/cmd/dst"
	"github.com/resonatehq/resonate/cmd/promises"
	"github.com/resonatehq/resonate/cmd/quickstart"
	"github.com/resonatehq/resonate/cmd/schedules"
	"github.com/resonatehq/resonate/cmd/serve"
	"github.com/resonatehq/resonate/cmd/tasks"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile string
)

var rootCmd = &cobra.Command{
	Use:     "resonate",
	Short:   "Resonate: distributed async await",
	Version: "0.7.0", // This needs to be bumped when new versions are released.
}

func init() {
	cobra.OnInitialize(initConfig)

	// Flags
	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "", "", "config file (default \"resonate.yml\")")
	rootCmd.PersistentFlags().StringP("log-level", "", "info", "can be one of: debug, info, warn, error")

	// Add Subcommands
	rootCmd.AddCommand(promises.NewCmd())
	rootCmd.AddCommand(schedules.NewCmd())
	rootCmd.AddCommand(dst.NewCmd())
	rootCmd.AddCommand(serve.ServeCmd())
	rootCmd.AddCommand(quickstart.NewCmd())
	rootCmd.AddCommand(tasks.NewCmd())

	// Set default output
	rootCmd.SetOut(os.Stdout)
	rootCmd.SetErr(os.Stderr)
}

func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		viper.SetConfigName("resonate")
		viper.AddConfigPath(".")
		viper.AddConfigPath("$HOME")
	}

	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			slog.Error("error reading config file", "error", err)
			os.Exit(1)
		}
	}
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

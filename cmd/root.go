package cmd

import (
	"log/slog"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgFile string

var rootCmd = &cobra.Command{
	Use:   "resonate",
	Short: "Durable promises and executions",
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (defaults to resonate.yml)")
	rootCmd.PersistentFlags().Int("log-level", 0, "log level")
	_ = viper.BindPFlag("logs.level", rootCmd.PersistentFlags().Lookup("log-level"))
	_ = viper.BindPFlag("dst.logs.level", rootCmd.PersistentFlags().Lookup("log-level"))
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
		slog.Error("error reading config file", "error", err)
		os.Exit(1)
	}
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

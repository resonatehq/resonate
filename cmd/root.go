package cmd

import (
	"github.com/resonatehq/resonate/internal/creds"
	"log/slog"
	"os"
	"strings"

	"github.com/resonatehq/resonate/cmd/dst"
	"github.com/resonatehq/resonate/cmd/promises"
	"github.com/resonatehq/resonate/cmd/quickstart"
	"github.com/resonatehq/resonate/cmd/schedules"
	"github.com/resonatehq/resonate/cmd/serve"
	"github.com/resonatehq/resonate/pkg/client"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	server, cfgFile, credFile string
)

var rootCmd = &cobra.Command{
	Use:   "resonate",
	Short: "Durable promises",
}

func init() {
	cobra.OnInitialize(initConfig)

	c := client.NewOrDie(&server)

	// Flags
	rootCmd.PersistentFlags().StringVarP(&server, "server", "", "http://127.0.0.1:8001", "Server address used by the client")
	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "", "", "Config file (default \"resonate.yml\")")
	rootCmd.PersistentFlags().StringP("log-level", "", "info", "Log level, can be one of: debug, info, warn, error")
	_ = viper.BindPFlag("log.level", rootCmd.PersistentFlags().Lookup("log-level"))
	_ = viper.BindPFlag("dst.log.level", rootCmd.PersistentFlags().Lookup("log-level"))

	// Add Subcommands
	rootCmd.AddCommand(promises.NewCmd(c))
	rootCmd.AddCommand(schedules.NewCmd(c))
	rootCmd.AddCommand(dst.NewCmd())
	rootCmd.AddCommand(serve.ServeCmd())
	rootCmd.AddCommand(quickstart.NewCmd())

	// Set default output
	rootCmd.SetOut(os.Stdout)
	rootCmd.SetErr(os.Stderr)
}

func initCreds() {
	if credFile != "" {
		viper.SetConfigFile(credFile)
	} else {
		viper.SetConfigName("sample-creds")
		viper.AddConfigPath(".")
		viper.AddConfigPath("$HOME")
	}

	err := viper.ReadInConfig()
	if err != nil {
		slog.Error("Error reading creds file", "error", err)
		return
	}

	err = viper.Unmarshal(&creds.CredsFromFile)
	if err != nil {
		slog.Error("Unable to decode creds from file into struct", "error", err)
		return
	}
}

func initConfig() {
	initCreds()
	viper.Reset()

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

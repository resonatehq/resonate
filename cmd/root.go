package cmd

import (
	"log/slog"
	"os"
	"strings"

	"github.com/resonatehq/resonate/cmd/create"
	"github.com/resonatehq/resonate/cmd/delete"
	"github.com/resonatehq/resonate/cmd/describe"
	"github.com/resonatehq/resonate/cmd/get"
	"github.com/resonatehq/resonate/cmd/patch"
	"github.com/resonatehq/resonate/pkg/client"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var API string = "http://0.0.0.0:8001"

var cfgFile string

var rootCmd = &cobra.Command{
	Use:   "resonate",
	Short: "Durable promises and executions",
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&API, "api", API, "API server address")
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (defaults to resonate.yml)")
	rootCmd.PersistentFlags().String("log-level", "info", "log level, Options: debug, info, warn, error.")
	_ = viper.BindPFlag("log.level", rootCmd.PersistentFlags().Lookup("log-level"))
	_ = viper.BindPFlag("dst.log.level", rootCmd.PersistentFlags().Lookup("log-level"))

	c := client.NewOrDie(API)

	// Add Subcommands
	rootCmd.AddCommand(patch.NewCmdPatch(c))
	rootCmd.AddCommand(create.NewCmdCreate(c))
	rootCmd.AddCommand(delete.NewCmdDelete(c))
	rootCmd.AddCommand(describe.NewCmdDescribe(c))
	rootCmd.AddCommand(get.NewCmdGet(c))
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

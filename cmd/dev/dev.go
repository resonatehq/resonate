package dev

import (
	"strings"

	"github.com/go-viper/mapstructure/v2"
	"github.com/resonatehq/resonate/cmd/config"
	"github.com/resonatehq/resonate/cmd/serve"
	"github.com/resonatehq/resonate/cmd/util"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func NewCmd() *cobra.Command {
	var (
		v      = viper.New()
		config = &config.Config{}
	)

	cmd := &cobra.Command{
		Use:   "dev",
		Short: "Start Resonate server in development mode",
		Long:  "Start Resonate server with development-friendly defaults (in-memory SQLite store).\n\nThis command is an alias for: resonate serve --aio-store-sqlite-path ':memory:' --aio-store-postgres-query 'sslmode=disable'\n",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if file, _ := cmd.Flags().GetString("config"); file != "" {
				v.SetConfigFile(file)
			} else {
				v.SetConfigName("resonate-dev")
				v.AddConfigPath(".")
				v.AddConfigPath("$HOME")
			}

			v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
			v.AutomaticEnv()

			if err := v.ReadInConfig(); err != nil {
				if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
					return err
				}
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			hooks := mapstructure.ComposeDecodeHookFunc(
				util.MapToBytes(),
				mapstructure.StringToTimeDurationHookFunc(),
				mapstructure.StringToSliceHookFunc(","),
			)

			if err := v.Unmarshal(&config, viper.DecodeHook(hooks)); err != nil {
				return err
			}

			return serve.Serve(config)
		},
	}

	// bind config file flag
	cmd.Flags().StringP("config", "c", "", "config file (default resonate-dev.yaml)")

	// bind config
	_ = config.Bind(cmd, v, "dev")
	cmd.Flags().SortFlags = false

	return cmd
}

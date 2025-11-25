package dev

import (
	"fmt"
	"strings"

	"github.com/go-viper/mapstructure/v2"
	"github.com/resonatehq/resonate/cmd/config"
	"github.com/resonatehq/resonate/cmd/serve"
	"github.com/resonatehq/resonate/cmd/util"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func NewCmd(cfg *config.Config) *cobra.Command {
	var (
		vip = viper.New()
	)

	cmd := &cobra.Command{
		Use:   "dev",
		Short: "Start Resonate server in development mode",
		Long:  "Start Resonate server with development-friendly defaults (in-memory SQLite store).\n\nThis command is an alias for: resonate serve --aio-store-sqlite-path ':memory:' --aio-store-postgres-query 'sslmode=disable'\n",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if file, _ := cmd.Flags().GetString("config"); file != "" {
				vip.SetConfigFile(file)
			} else {
				vip.SetConfigName("resonate-dev")
				vip.AddConfigPath(".")
				vip.AddConfigPath("$HOME")
			}

			vip.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
			vip.AutomaticEnv()

			if err := vip.ReadInConfig(); err != nil {
				if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
					return err
				}
			}

			// flipy flip
			sFlag := cmd.PersistentFlags().Lookup("aio-store-sqlite-enable")
			pFlag := cmd.PersistentFlags().Lookup("aio-store-postgres-enable")
			if sFlag != nil && pFlag != nil && sFlag.Value.String() == "true" && pFlag.Value.String() == "true" {
				// postgres takes precedence
				_ = sFlag.Value.Set("false")
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			hooks := mapstructure.ComposeDecodeHookFunc(
				util.MapToBytes(),
				mapstructure.StringToTimeDurationHookFunc(),
				mapstructure.StringToSliceHookFunc(","),
			)

			// decode config
			if err := vip.Unmarshal(&cfg, viper.DecodeHook(hooks)); err != nil {
				return err
			}

			// decode plugins
			for _, plugin := range cfg.Plugins() {
				value, ok := util.Extract(vip.AllSettings(), plugin.Key())
				if !ok {
					panic("plugin config not found")
				}
				if err := plugin.Decode(value, hooks); err != nil {
					return err
				}
			}

			return serve.Serve(cfg)
		},
	}

	// bind config file flag
	cmd.Flags().StringP("config", "c", "", "config file (default resonate-dev.yaml)")

	// bind config
	util.Bind(cfg, cmd, cmd.Flags(), vip, cmd.Name())

	// bind plugins
	for _, plugin := range cfg.Plugins() {
		enabled := fmt.Sprintf("%s-enabled", plugin.Prefix())
		cmd.Flags().BoolVar(plugin.EnabledP(), enabled, plugin.Enabled(), "enable plugin")
		_ = vip.BindPFlag(fmt.Sprintf("%s.enabled", plugin.Key()), cmd.Flags().Lookup(enabled))

		plugin.Bind(cmd, cmd.Flags(), vip, cmd.Name(), plugin.Prefix(), plugin.Key())
	}

	cmd.Flags().SortFlags = false
	return cmd
}

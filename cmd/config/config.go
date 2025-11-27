package config

import (
	"fmt"
	"math/rand" // nosemgrep
	"os"
	"strings"

	"github.com/go-viper/mapstructure/v2"
	"github.com/resonatehq/resonate/cmd/util"
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/resonatehq/resonate/internal/plugins"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Config

type Config struct {
	System      system.Config `flag:"system"`
	API         API           `flag:"api"`
	AIO         AIO           `flag:"aio"`
	MetricsAddr string        `flag:"metrics-addr" desc:"prometheus metrics server address" default:":9090"`
	LogLevel    string        `flag:"log-level" desc:"can be one of: debug, info, warn, error" default:"info"`
}

func (c *Config) Plugins() []plugin {
	plugins := []plugin{}
	plugins = append(plugins, c.API.Subsystems.AsPlugins()...)
	plugins = append(plugins, c.AIO.Subsystems.AsPlugins()...)
	plugins = append(plugins, c.AIO.Plugins.AsPlugins()...)

	return plugins
}

type API struct {
	Size       int           `flag:"size" desc:"submission buffered channel size" default:"1000" dst:"1:1000"`
	Auth       Auth          `flag:"auth"`
	Subsystems APISubsystems `flag:"-"`
}

type AIO struct {
	Size       int           `flag:"size" desc:"completion buffered channel size" default:"1000" dst:"1:1000"`
	Subsystems AIOSubsystems `flag:"-"`
	Plugins    AIOPlugins    `flag:"-"`
}

type Auth struct {
	PublicKey string `flag:"public-key" desc:"public key path used for jwt based authentication"`
}

func (a *API) Middleware() ([]api.Middleware, error) {
	middleware := []api.Middleware{}

	if a.Auth.PublicKey != "" {
		pem, err := os.ReadFile(a.Auth.PublicKey)
		if err != nil {
			return nil, err
		}
		m, err := api.NewJWTAuthenticator(pem)
		if err != nil {
			return nil, err
		}
		middleware = append(middleware, m)
	}

	return middleware, nil
}

// Plugins

type plugin interface {
	Bind(*cobra.Command, *pflag.FlagSet, *viper.Viper, string)
	Decode(*viper.Viper, mapstructure.DecodeHookFunc) error
}

type enabledFlag struct {
	key string
	val bool
}

type APISubsystems struct {
	subsystems []*apiSubsystem `mapstructure:"-"`
}

func (a *APISubsystems) Add(name string, enabled bool, subsystem APISubsystem) {
	kName := strings.ReplaceAll(name, "-", ".")
	a.subsystems = append(a.subsystems, &apiSubsystem{
		prefix:    fmt.Sprintf("api-%s", name),
		key:       fmt.Sprintf("api.subsystems.%s.config", kName),
		name:      name,
		enabled:   &enabledFlag{fmt.Sprintf("api.subsystems.%s.enabled", kName), enabled},
		subsystem: subsystem,
	})
}

func (a *APISubsystems) All() []*apiSubsystem {
	return a.subsystems
}

func (a *APISubsystems) AsPlugins() []plugin {
	subsystems := []plugin{}
	for _, s := range a.subsystems {
		subsystems = append(subsystems, s)
	}

	return subsystems
}

type apiSubsystem struct {
	subsystem APISubsystem
	prefix    string
	key       string
	name      string
	enabled   *enabledFlag
}

type APISubsystem interface {
	Bind(*cobra.Command, *pflag.FlagSet, *viper.Viper, string, string, string)
	Decode(any, mapstructure.DecodeHookFunc) error
	New(api.API, *metrics.Metrics) (api.Subsystem, error)
}

func (a *apiSubsystem) Name() string {
	return a.name
}

func (a *apiSubsystem) Enabled() bool {
	return a.enabled.val
}

func (a *apiSubsystem) Bind(cmd *cobra.Command, flg *pflag.FlagSet, vip *viper.Viper, name string) {
	n := fmt.Sprintf("%s-enable", a.prefix)
	flg.Bool(n, a.enabled.val, "enable subsystem")
	_ = vip.BindPFlag(a.enabled.key, flg.Lookup(n))

	a.subsystem.Bind(cmd, flg, vip, name, a.prefix, a.key)
}

func (a *apiSubsystem) Decode(vip *viper.Viper, hooks mapstructure.DecodeHookFunc) error {
	value, ok := util.Extract(vip.AllSettings(), a.key)
	if !ok {
		panic("plugin config not found")
	}

	a.enabled.val = vip.GetBool(a.enabled.key)
	return a.subsystem.Decode(value, hooks)
}

func (a *apiSubsystem) New(api api.API, metrics *metrics.Metrics) (api.Subsystem, error) {
	return a.subsystem.New(api, metrics)
}

type AIOSubsystems struct {
	subsystems []*aioSubsystem `mapstructure:"-"`
}

func (a *AIOSubsystems) Add(name string, enabled bool, subsystem AIOSubsystem) {
	kName := strings.ReplaceAll(name, "-", ".")
	a.subsystems = append(a.subsystems, &aioSubsystem{
		subsystem: subsystem,
		prefix:    fmt.Sprintf("aio-%s", name),
		key:       fmt.Sprintf("aio.subsystems.%s.config", kName),
		name:      name,
		enabled:   &enabledFlag{fmt.Sprintf("aio.subsystems.%s.enabled", kName), enabled},
	})
}

func (a *AIOSubsystems) All() []*aioSubsystem {
	return a.subsystems
}

func (a *AIOSubsystems) AsPlugins() []plugin {
	subsystems := []plugin{}
	for _, s := range a.subsystems {
		subsystems = append(subsystems, s)
	}

	return subsystems
}

type aioSubsystem struct {
	subsystem AIOSubsystem
	prefix    string
	key       string
	name      string
	enabled   *enabledFlag
}

type AIOSubsystem interface {
	Bind(*cobra.Command, *pflag.FlagSet, *viper.Viper, string, string, string)
	Decode(any, mapstructure.DecodeHookFunc) error
	New(aio.AIO, *metrics.Metrics) (aio.Subsystem, error)
	NewDST(aio.AIO, *metrics.Metrics, *rand.Rand, chan any) (aio.SubsystemDST, error)
}

func (a *aioSubsystem) Name() string {
	return a.name
}

func (a *aioSubsystem) Enabled() bool {
	return a.enabled.val
}

func (a *aioSubsystem) Bind(cmd *cobra.Command, flg *pflag.FlagSet, vip *viper.Viper, name string) {
	n := fmt.Sprintf("%s-enable", a.prefix)
	flg.Bool(n, a.enabled.val, "enable subsystem")
	_ = vip.BindPFlag(a.enabled.key, flg.Lookup(n))

	a.subsystem.Bind(cmd, flg, vip, name, a.prefix, a.key)
}

func (a *aioSubsystem) Decode(vip *viper.Viper, hooks mapstructure.DecodeHookFunc) error {
	value, ok := util.Extract(vip.AllSettings(), a.key)
	if !ok {
		panic("plugin config not found")
	}

	a.enabled.val = vip.GetBool(a.enabled.key)
	return a.subsystem.Decode(value, hooks)
}

func (a *aioSubsystem) New(aio aio.AIO, metrics *metrics.Metrics) (aio.Subsystem, error) {
	return a.subsystem.New(aio, metrics)
}

func (a *aioSubsystem) NewDST(aio aio.AIO, metrics *metrics.Metrics, r *rand.Rand, c chan any) (aio.SubsystemDST, error) {
	return a.subsystem.NewDST(aio, metrics, r, c)
}

type AIOPlugins struct {
	plugins []*aioPlugin `mapstructure:"-"`
}

func (a *AIOPlugins) Add(name string, enabled bool, plugin AIOPlugin) {
	kName := strings.ReplaceAll(name, "-", ".")
	a.plugins = append(a.plugins, &aioPlugin{
		plugin:  plugin,
		prefix:  fmt.Sprintf("aio-%s", name),
		key:     fmt.Sprintf("aio.plugins.%s.config", kName),
		name:    name,
		enabled: &enabledFlag{fmt.Sprintf("aio.plugins.%s.enabled", kName), enabled},
	})
}

func (a *AIOPlugins) All() []*aioPlugin {
	return a.plugins
}

func (a *AIOPlugins) AsPlugins() []plugin {
	plugins := []plugin{}
	for _, p := range a.plugins {
		plugins = append(plugins, p)
	}

	return plugins
}

type aioPlugin struct {
	plugin  AIOPlugin
	prefix  string
	key     string
	name    string
	enabled *enabledFlag
}

type AIOPlugin interface {
	Bind(*cobra.Command, *pflag.FlagSet, *viper.Viper, string, string, string)
	Decode(any, mapstructure.DecodeHookFunc) error
	New(*metrics.Metrics) (plugins.Plugin, error)
}

func (a *aioPlugin) Name() string {
	return a.name
}

func (a *aioPlugin) Enabled() bool {
	return a.enabled.val
}

func (a *aioPlugin) Bind(cmd *cobra.Command, flg *pflag.FlagSet, vip *viper.Viper, name string) {
	n := fmt.Sprintf("%s-enable", a.prefix)
	flg.Bool(n, a.enabled.val, "enable plugin")
	_ = vip.BindPFlag(a.enabled.key, flg.Lookup(n))

	a.enabled.val = vip.GetBool(a.enabled.key)
	a.plugin.Bind(cmd, flg, vip, name, a.prefix, a.key)
}

func (a *aioPlugin) Decode(vip *viper.Viper, hooks mapstructure.DecodeHookFunc) error {
	value, ok := util.Extract(vip.AllSettings(), a.key)
	if !ok {
		panic("plugin config not found")
	}

	return a.plugin.Decode(value, hooks)
}

func (a *aioPlugin) New(metrics *metrics.Metrics) (plugins.Plugin, error) {
	return a.plugin.New(metrics)
}

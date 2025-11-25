package config

import (
	"fmt"
	"math/rand" // nosemgrep
	"os"
	"strings"

	"github.com/go-viper/mapstructure/v2"
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
	Subsystems apiSubsystems `mapstructure:"-"`
}

type AIO struct {
	Size       int           `flag:"size" desc:"completion buffered channel size" default:"1000" dst:"1:1000"`
	Subsystems aioSubsystems `mapstructure:"-"`
	Plugins    aioPlugins    `mapstructure:"-"`
}

type Auth struct {
	PublicKeyPath string `flag:"public-key" desc:"public key path used for jwt based authentication"`
}

func (a *API) Middleware() ([]api.Middleware, error) {
	middleware := []api.Middleware{}

	if a.Auth.PublicKeyPath != "" {
		pem, err := os.ReadFile(a.Auth.PublicKeyPath)
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

type apiSubsystems []*apiSubsystem
type aioSubsystems []*aioSubsystem
type aioPlugins []*aioPlugin

type apiSubsystem struct {
	Plugin  iAPISubsystem
	prefix  string
	key     string
	name    string
	enabled bool
}

func (a *apiSubsystem) Prefix() string {
	return a.prefix
}

func (a *apiSubsystem) Key() string {
	return a.key
}

func (a *apiSubsystem) Name() string {
	return a.name
}

func (a *apiSubsystem) Enabled() bool {
	return a.enabled
}

func (a *apiSubsystem) EnabledP() *bool {
	return &a.enabled
}

func (a *apiSubsystem) Config() config {
	return a.Plugin
}

type aioSubsystem struct {
	Plugin  iAIOSubsystem
	prefix  string
	key     string
	name    string
	enabled bool
}

func (a *aioSubsystem) Prefix() string {
	return a.prefix
}

func (a *aioSubsystem) Key() string {
	return a.key
}

func (a *aioSubsystem) Name() string {
	return a.name
}

func (a *aioSubsystem) Enabled() bool {
	return a.enabled
}

func (a *aioSubsystem) EnabledP() *bool {
	return &a.enabled
}

func (a *aioSubsystem) Config() config {
	return a.Plugin
}

type aioPlugin struct {
	Plugin  iAIOPlugin
	prefix  string
	key     string
	name    string
	enabled bool
}

func (a *aioPlugin) Prefix() string {
	return a.prefix
}

func (a *aioPlugin) Key() string {
	return a.key
}

func (a *aioPlugin) Name() string {
	return a.name
}

func (a *aioPlugin) Enabled() bool {
	return a.enabled
}

func (a *aioPlugin) EnabledP() *bool {
	return &a.enabled
}

func (a *aioPlugin) Config() config {
	return a.Plugin
}

type plugin interface {
	Config() config
	Prefix() string
	Key() string
	Name() string
	Enabled() bool
	EnabledP() *bool
}

type config interface {
	Bind(*cobra.Command, *pflag.FlagSet, *viper.Viper, string, string, string)
	Decode(any, mapstructure.DecodeHookFunc) error
}

type iAPISubsystem interface {
	Bind(*cobra.Command, *pflag.FlagSet, *viper.Viper, string, string, string)
	Decode(any, mapstructure.DecodeHookFunc) error
	New(api.API, *metrics.Metrics) (api.Subsystem, error)
}

type iAIOSubsystem interface {
	Bind(*cobra.Command, *pflag.FlagSet, *viper.Viper, string, string, string)
	Decode(any, mapstructure.DecodeHookFunc) error
	New(aio.AIO, *metrics.Metrics) (aio.Subsystem, error)
	NewDST(aio.AIO, *metrics.Metrics, *rand.Rand, chan any) (aio.SubsystemDST, error)
}

type iAIOPlugin interface {
	Bind(*cobra.Command, *pflag.FlagSet, *viper.Viper, string, string, string)
	Decode(any, mapstructure.DecodeHookFunc) error
	New(*metrics.Metrics) (plugins.Plugin, error)
}

func (a *apiSubsystems) Add(name string, enabled bool, subsystem iAPISubsystem) {
	*a = append(*a, &apiSubsystem{
		Plugin:  subsystem,
		prefix:  fmt.Sprintf("api-%s", name),
		key:     fmt.Sprintf("api.subsystems.%s.config", strings.ReplaceAll(name, "-", ".")),
		name:    name,
		enabled: enabled,
	})
}

func (a *apiSubsystems) AsPlugins() []plugin {
	result := make([]plugin, len(*a))
	for i, subsystem := range *a {
		result[i] = subsystem
	}

	return result
}

func (a *aioSubsystems) Add(name string, enabled bool, subsystem iAIOSubsystem) {
	*a = append(*a, &aioSubsystem{
		Plugin:  subsystem,
		prefix:  fmt.Sprintf("aio-%s", name),
		key:     fmt.Sprintf("aio.subsystems.%s.config", strings.ReplaceAll(name, "-", ".")),
		name:    name,
		enabled: enabled,
	})
}

func (a *aioSubsystems) AsPlugins() []plugin {
	result := make([]plugin, len(*a))
	for i, subsystem := range *a {
		result[i] = subsystem
	}

	return result
}

func (a *aioPlugins) Add(name string, enabled bool, plugin iAIOPlugin) {
	*a = append(*a, &aioPlugin{
		Plugin:  plugin,
		prefix:  fmt.Sprintf("aio-sender-plugin-%s", name),
		key:     fmt.Sprintf("aio.subsystems.sender.plugins.%s.config", strings.ReplaceAll(name, "-", ".")),
		name:    name,
		enabled: enabled,
	})
}

func (a *aioPlugins) AsPlugins() []plugin {
	result := make([]plugin, len(*a))
	for i, plugin := range *a {
		result[i] = plugin
	}

	return result
}

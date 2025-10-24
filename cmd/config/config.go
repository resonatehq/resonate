package config

import (
	"encoding/json"
	"fmt"
	"math/rand" // nosemgrep
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/resonatehq/resonate/cmd/util"
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/api"
	httpPlugin "github.com/resonatehq/resonate/internal/app/plugins/http"
	"github.com/resonatehq/resonate/internal/app/plugins/kafka"
	"github.com/resonatehq/resonate/internal/app/plugins/poll"
	"github.com/resonatehq/resonate/internal/app/plugins/sqs"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/echo"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/router"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/sender"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/postgres"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/sqlite"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/grpc"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/http"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type Config config[API, AIO]
type ConfigDST config[APIDST, AIODST]

type t_api interface {
	API | APIDST
}

type t_aio interface {
	AIO | AIODST
}

type config[T t_api, U t_aio] struct {
	System      system.Config `flag:"system"`
	API         T             `flag:"api"`
	AIO         U             `flag:"aio"`
	MetricsAddr string        `flag:"metrics-addr" desc:"prometheus metrics server address" default:":9090"`
	LogLevel    string        `flag:"log-level" desc:"can be one of: debug, info, warn, error" default:"info"`
}

func (c *Config) Bind(cmd *cobra.Command, vip *viper.Viper, tag string) error {
	return Bind(cmd, c, vip, tag, "", "")
}

func (c *ConfigDST) Bind(cmd *cobra.Command, vip *viper.Viper) error {
	return Bind(cmd, c, vip, "dst", "", "")
}

type API struct {
	Size       int           `flag:"size" desc:"submission buffered channel size" default:"1000" dst:"1:1000"`
	Subsystems APISubsystems `flag:"-"`
}

type APIDST struct {
	Size       int              `flag:"size" desc:"submission buffered channel size" default:"1000" dst:"1:1000"`
	Subsystems APIDSTSubsystems `flag:"-"`
}

type AIO struct {
	Size       int           `flag:"size" desc:"completion buffered channel size" default:"1000" dst:"1:1000"`
	Subsystems AIOSubsystems `flag:"-"`
}

type AIODST struct {
	Size       int              `flag:"size" desc:"completion buffered channel size" default:"1000" dst:"1:1000"`
	Subsystems AIODSTSubsystems `flag:"-"`
}

type APISubsystems struct {
	Http EnabledSubsystem[http.Config] `flag:"http"`
	Grpc EnabledSubsystem[grpc.Config] `flag:"grpc"`
}

type AIOSubsystems struct {
	Echo          DisabledSubsystem[echo.Config]     `flag:"echo"`
	Router        EnabledSubsystem[router.Config]    `flag:"router"`
	Sender        EnabledSubsystem[sender.Config]    `flag:"sender"`
	StorePostgres DisabledSubsystem[postgres.Config] `flag:"store-postgres"`
	StoreSqlite   EnabledSubsystem[sqlite.Config]    `flag:"store-sqlite"`
}

type EnabledSubsystem[T any] struct {
	Enabled bool `flag:"enable" desc:"enable subsystem" default:"true"`
	Config  T    `flag:"-"`
}

type DisabledSubsystem[T any] struct {
	Enabled bool `flag:"enable" desc:"enable subsystem" default:"false"`
	Config  T    `flag:"-"`
}

func (c *Config) APISubsystems(a api.API, pollAddr string) ([]api.Subsystem, error) {
	subsystems := []api.Subsystem{}
	if c.API.Subsystems.Http.Enabled {
		subsystem, err := http.New(a, &c.API.Subsystems.Http.Config, pollAddr)
		if err != nil {
			return nil, err
		}

		subsystems = append(subsystems, subsystem)
	}
	if c.API.Subsystems.Grpc.Enabled {
		subsystem, err := grpc.New(a, &c.API.Subsystems.Grpc.Config)
		if err != nil {
			return nil, err
		}

		subsystems = append(subsystems, subsystem)
	}

	return subsystems, nil
}

func (c *Config) AIOSubsystems(a aio.AIO, metrics *metrics.Metrics, plugins []aio.Plugin) ([]aio.Subsystem, error) {
	subsystems := []aio.Subsystem{}
	if c.AIO.Subsystems.Echo.Enabled {
		subsystem, err := echo.New(a, metrics, &c.AIO.Subsystems.Echo.Config)
		if err != nil {
			return nil, err
		}

		subsystems = append(subsystems, subsystem)
	}
	if c.AIO.Subsystems.Router.Enabled {
		subsystem, err := router.New(a, metrics, &c.AIO.Subsystems.Router.Config)
		if err != nil {
			return nil, err
		}

		subsystems = append(subsystems, subsystem)
	}
	if c.AIO.Subsystems.Sender.Enabled {
		subsystem, err := sender.New(a, metrics, &c.AIO.Subsystems.Sender.Config, plugins)
		if err != nil {
			return nil, err
		}

		subsystems = append(subsystems, subsystem)
	}

	subsystem, err := c.store(a, metrics)
	if err != nil {
		return nil, err
	}

	subsystems = append(subsystems, subsystem)
	return subsystems, nil
}

func (c *Config) AIOPlugins(a aio.AIO, metrics *metrics.Metrics) ([]aio.Plugin, string, error) {
	plugins := []aio.Plugin{}
	var pollAddr string

	if c.AIO.Subsystems.Sender.Config.Plugins.Poll.Enabled {
		plugin, err := poll.New(a, metrics, &c.AIO.Subsystems.Sender.Config.Plugins.Poll.Config)
		if err != nil {
			return nil, "", err
		}

		pollAddr = plugin.Addr() // grab the address to pass to the API
		plugins = append(plugins, plugin)
	}
	if c.AIO.Subsystems.Sender.Config.Plugins.Http.Enabled {
		plugin, err := httpPlugin.New(a, metrics, &c.AIO.Subsystems.Sender.Config.Plugins.Http.Config)
		if err != nil {
			return nil, "", err
		}

		plugins = append(plugins, plugin)
	}
	if c.AIO.Subsystems.Sender.Config.Plugins.SQS.Enabled {
		plugin, err := sqs.New(a, metrics, &c.AIO.Subsystems.Sender.Config.Plugins.SQS.Config)
		if err != nil {
			return nil, "", err
		}

		plugins = append(plugins, plugin)
	}
	if c.AIO.Subsystems.Sender.Config.Plugins.Kafka.Enabled {
		plugin, err := kafka.New(a, metrics, &c.AIO.Subsystems.Sender.Config.Plugins.Kafka.Config)
		if err != nil {
			return nil, "", err
		}

		plugins = append(plugins, plugin)
	}

	return plugins, pollAddr, nil
}

func (c *Config) store(a aio.AIO, metrics *metrics.Metrics) (aio.Subsystem, error) {
	if c.AIO.Subsystems.StorePostgres.Enabled {
		return postgres.New(a, metrics, &c.AIO.Subsystems.StorePostgres.Config)
	} else if c.AIO.Subsystems.StoreSqlite.Enabled {
		return sqlite.New(a, metrics, &c.AIO.Subsystems.StoreSqlite.Config)
	}

	return nil, fmt.Errorf("no store enabled")
}

type APIDSTSubsystems struct {
	Http DisabledSubsystem[http.Config] `flag:"http"`
	Grpc DisabledSubsystem[grpc.Config] `flag:"grpc"`
}

type AIODSTSubsystems struct {
	Router        EnabledSubsystem[router.Config]    `flag:"router"`
	Sender        EnabledSubsystem[sender.ConfigDST] `flag:"sender"`
	StorePostgres DisabledSubsystem[postgres.Config] `flag:"store-postgres"`
	StoreSqlite   EnabledSubsystem[sqlite.Config]    `flag:"store-sqlite"`
}

func (c *ConfigDST) APISubsystems(a api.API, pollAddr string) ([]api.Subsystem, error) {
	subsystems := []api.Subsystem{}
	if c.API.Subsystems.Http.Enabled {
		subsystem, err := http.New(a, &c.API.Subsystems.Http.Config, pollAddr)
		if err != nil {
			return nil, err
		}

		subsystems = append(subsystems, subsystem)
	}
	if c.API.Subsystems.Grpc.Enabled {
		subsystem, err := grpc.New(a, &c.API.Subsystems.Grpc.Config)
		if err != nil {
			return nil, err
		}

		subsystems = append(subsystems, subsystem)
	}

	return subsystems, nil
}

func (c *ConfigDST) AIOSubsystems(a aio.AIO, metrics *metrics.Metrics, r *rand.Rand, backchannel chan interface{}) ([]aio.SubsystemDST, error) {
	subsystems := []aio.SubsystemDST{}
	if c.AIO.Subsystems.Router.Enabled {
		subsystem, err := router.New(a, metrics, &c.AIO.Subsystems.Router.Config)
		if err != nil {
			return nil, err
		}

		subsystems = append(subsystems, subsystem)
	}
	if c.AIO.Subsystems.Sender.Enabled {
		subsystem, err := sender.NewDST(r, backchannel, &c.AIO.Subsystems.Sender.Config)
		if err != nil {
			return nil, err
		}

		subsystems = append(subsystems, subsystem)
	}

	subsystem, err := c.store(a, metrics)
	if err != nil {
		return nil, err
	}

	subsystems = append(subsystems, subsystem)
	return subsystems, nil
}

func (c *ConfigDST) store(a aio.AIO, metrics *metrics.Metrics) (aio.SubsystemDST, error) {
	if c.AIO.Subsystems.StorePostgres.Enabled {
		return postgres.New(a, metrics, &c.AIO.Subsystems.StorePostgres.Config)
	} else if c.AIO.Subsystems.StoreSqlite.Enabled {
		return sqlite.New(a, metrics, &c.AIO.Subsystems.StoreSqlite.Config)
	}

	return nil, fmt.Errorf("no store enabled")
}

// Helper functions

// Bind binds configuration struct fields to cobra flags and viper config
func Bind(cmd *cobra.Command, cfg any, vip *viper.Viper, tag string, fPrefix string, kPrefix string) error {
	v := reflect.ValueOf(cfg).Elem()
	t := v.Type()

	for i := range v.NumField() {
		field := t.Field(i)
		flag := field.Tag.Get("flag")
		desc := field.Tag.Get("desc")
		persistent := field.Tag.Get("persistent")

		flags := cmd.Flags()
		if persistent == "true" {
			flags = cmd.PersistentFlags()
		}

		var value string
		if v := field.Tag.Get(tag); v != "" {
			value = v
		} else {
			value = field.Tag.Get("default")
		}

		var n string
		if fPrefix == "" {
			n = flag
		} else if flag == "-" {
			n = fPrefix
		} else {
			n = fmt.Sprintf("%s-%s", fPrefix, flag)
		}

		var k string
		if kPrefix == "" {
			k = field.Name
		} else {
			k = fmt.Sprintf("%s.%s", kPrefix, field.Name)
		}

		switch field.Type.Kind() {
		case reflect.String:
			flags.String(n, value, desc)
			_ = vip.BindPFlag(k, flags.Lookup(n))
		case reflect.Bool:
			flags.Bool(n, value == "true", desc)
			_ = vip.BindPFlag(k, flags.Lookup(n))
		case reflect.Int:
			if strings.Contains(value, ":") {
				flag := util.NewRangeIntFlag(0, 0)
				if err := flag.Set(value); err != nil {
					return err
				}

				flags.Var(flag, n, desc)
				_ = vip.BindPFlag(k, flags.Lookup(n))
			} else {
				v, _ := strconv.Atoi(value)
				flags.Int(n, v, desc)
				_ = vip.BindPFlag(k, flags.Lookup(n))
			}
		case reflect.Int64:
			if field.Type == reflect.TypeOf(time.Duration(0)) {
				if strings.Contains(value, ":") {
					flag := util.NewRangeDurationFlag(0, 0)
					if err := flag.Set(value); err != nil {
						return err
					}

					flags.Var(flag, n, desc)
					_ = vip.BindPFlag(k, flags.Lookup(n))
				} else {
					v, _ := time.ParseDuration(value)
					flags.Duration(n, v, desc)
					_ = vip.BindPFlag(k, flags.Lookup(n))
				}
			} else {
				if strings.Contains(value, ":") {
					flag := util.NewRangeInt64Flag(0, 0)
					if err := flag.Set(value); err != nil {
						return err
					}

					flags.Var(flag, n, desc)
					_ = vip.BindPFlag(k, flags.Lookup(n))
				} else {
					v, _ := strconv.ParseInt(value, 10, 64)
					flags.Int64(n, v, desc)
					_ = vip.BindPFlag(k, flags.Lookup(n))
				}
			}
		case reflect.Float64:
			if strings.Contains(value, ":") {
				flag := util.NewRangeFloat64Flag(0, 0)
				if err := flag.Set(value); err != nil {
					return err
				}

				flags.Var(flag, n, desc)
				_ = vip.BindPFlag(k, flags.Lookup(n))
			} else {
				v, _ := strconv.ParseFloat(value, 64)
				flags.Float64(n, v, desc)
				_ = vip.BindPFlag(k, flags.Lookup(n))
			}
		case reflect.Slice:
			// TODO: support additional slice types via flags
			if field.Type != reflect.TypeOf([]string{}) {
				continue
			}

			var v []string
			if value == "" {
				v = []string{}
			} else {
				v = []string{value}
			}
			flags.StringArray(n, v, desc)
			_ = vip.BindPFlag(k, flags.Lookup(n))
		case reflect.Map:
			if field.Type != reflect.TypeOf(map[string]string{}) {
				panic(fmt.Sprintf("unsupported map type: %s", field.Type.Kind()))
			}
			if value == "" {
				value = "{}"
			}
			var v map[string]string
			if err := json.Unmarshal([]byte(value), &v); err != nil {
				return err
			}
			flags.StringToString(n, v, desc)
			_ = vip.BindPFlag(k, flags.Lookup(n))
		case reflect.Struct:
			if err := Bind(cmd, v.Field(i).Addr().Interface(), vip, tag, n, k); err != nil {
				return err
			}
		default:
			panic(fmt.Sprintf("unsupported type %s", field.Type.Kind()))
		}
	}

	return nil
}

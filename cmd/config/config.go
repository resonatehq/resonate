package config

import (
	"encoding/json"
	"fmt"
	"math/rand" // nosemgrep
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/resonatehq/resonate/cmd/util"
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/echo"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/match"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queue"
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
	MetricsPort int           `flag:"metrics-port" desc:"prometheus metrics server port" default:"9090"`
	LogLevel    string        `flag:"log-level" desc:"can be one of: debug, info, warn, error" default:"info"`
}

func (c *Config) Bind(cmd *cobra.Command) error {
	return bind(cmd, c, false, "", "")
}

func (c *ConfigDST) BindDST(cmd *cobra.Command) error {
	return bind(cmd, c, true, "", "dst")
}

func (c *Config) Parse() error {
	hooks := mapstructure.ComposeDecodeHookFunc(
		util.MapToBytes(),
		mapstructure.StringToTimeDurationHookFunc(),
		mapstructure.StringToSliceHookFunc(","),
	)

	if err := viper.Unmarshal(&c, viper.DecodeHook(hooks)); err != nil {
		return err
	}

	// complex defaults
	if c.System.Url == "" {
		host := c.API.Subsystems.Http.Config.Host
		if host == "0.0.0.0" {
			host = "127.0.0.1"
		}
		c.System.Url = fmt.Sprintf("http://%s:%d", host, c.API.Subsystems.Http.Config.Port)
	}

	return nil
}

func (c *ConfigDST) Parse(r *rand.Rand) error {
	hooks := mapstructure.ComposeDecodeHookFunc(
		util.StringToRange(r),
		util.MapToBytes(),
		mapstructure.StringToTimeDurationHookFunc(),
		mapstructure.StringToSliceHookFunc(","),
	)

	config := struct{ DST *ConfigDST }{DST: c}
	if err := viper.Unmarshal(&config, viper.DecodeHook(hooks)); err != nil {
		return err
	}

	return nil
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
	Queue         EnabledSubsystem[queue.Config]     `flag:"queue"`
	Match         EnabledSubsystem[match.Config]     `flag:"match"`
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

func (s *APISubsystems) Instantiate(a api.API) []api.Subsystem {
	subsystems := []api.Subsystem{}
	if s.Http.Enabled {
		subsystems = append(subsystems, http.New(a, &s.Http.Config))
	}
	if s.Grpc.Enabled {
		subsystems = append(subsystems, grpc.New(a, &s.Grpc.Config))
	}

	return subsystems
}

func (s *AIOSubsystems) Instantiate(a aio.AIO, metrics *metrics.Metrics) ([]aio.Subsystem, error) {
	subsystems := []aio.Subsystem{}
	if s.Echo.Enabled {
		subsystem, err := echo.New(a, metrics, &s.Echo.Config)
		if err != nil {
			return nil, err
		}

		subsystems = append(subsystems, subsystem)
	}
	if s.Match.Enabled {
		subsystem, err := match.New(a, metrics, &s.Match.Config)
		if err != nil {
			return nil, err
		}

		subsystems = append(subsystems, subsystem)
	}
	if s.Queue.Enabled {
		subsystem, err := queue.New(a, metrics, &s.Queue.Config)
		if err != nil {
			return nil, err
		}

		subsystems = append(subsystems, subsystem)
	}

	subsystem, err := s.instantiateStore(a, metrics)
	if err != nil {
		return nil, err
	}

	subsystems = append(subsystems, subsystem)
	return subsystems, nil
}

func (s *AIOSubsystems) instantiateStore(a aio.AIO, metrics *metrics.Metrics) (aio.Subsystem, error) {
	if s.StorePostgres.Enabled {
		return postgres.New(a, metrics, &s.StorePostgres.Config)
	} else if s.StoreSqlite.Enabled {
		return sqlite.New(a, metrics, &s.StoreSqlite.Config)
	}
	return nil, fmt.Errorf("no store enabled")
}

type APIDSTSubsystems struct {
	Http DisabledSubsystem[http.Config] `flag:"http"`
	Grpc DisabledSubsystem[grpc.Config] `flag:"grpc"`
}

type AIODSTSubsystems struct {
	Queue         EnabledSubsystem[queue.ConfigDST]  `flag:"queue"`
	Match         EnabledSubsystem[match.Config]     `flag:"match"`
	StorePostgres DisabledSubsystem[postgres.Config] `flag:"store-postgres"`
	StoreSqlite   EnabledSubsystem[sqlite.Config]    `flag:"store-sqlite"`
}

func (s *APIDSTSubsystems) Instantiate(a api.API) []api.Subsystem {
	subsystems := []api.Subsystem{}
	if s.Http.Enabled {
		subsystems = append(subsystems, http.New(a, &s.Http.Config))
	}
	if s.Grpc.Enabled {
		subsystems = append(subsystems, grpc.New(a, &s.Grpc.Config))
	}

	return subsystems
}

func (s *AIODSTSubsystems) Instantiate(a aio.AIO, metrics *metrics.Metrics, r *rand.Rand, backchannel chan interface{}) ([]aio.SubsystemDST, error) {
	subsystems := []aio.SubsystemDST{}
	if s.Match.Enabled {
		subsystem, err := match.New(a, metrics, &s.Match.Config)
		if err != nil {
			return nil, err
		}

		subsystems = append(subsystems, subsystem)
	}
	if s.Queue.Enabled {
		subsystem, err := queue.NewDST(r, backchannel, &s.Queue.Config)
		if err != nil {
			return nil, err
		}

		subsystems = append(subsystems, subsystem)
	}

	subsystem, err := s.instantiateStore(a, metrics)
	if err != nil {
		return nil, err
	}

	subsystems = append(subsystems, subsystem)
	return subsystems, nil
}

func (s *AIODSTSubsystems) instantiateStore(a aio.AIO, metrics *metrics.Metrics) (aio.SubsystemDST, error) {
	if s.StorePostgres.Enabled {
		return postgres.New(a, metrics, &s.StorePostgres.Config)
	} else if s.StoreSqlite.Enabled {
		return sqlite.New(a, metrics, &s.StoreSqlite.Config)
	}
	return nil, fmt.Errorf("no store enabled")
}

// Helper functions

func bind(cmd *cobra.Command, cfg interface{}, dst bool, fPrefix string, kPrefix string) error {
	v := reflect.ValueOf(cfg).Elem()
	t := v.Type()

	for i := 0; i < v.NumField(); i++ {
		field := t.Field(i)
		flag := field.Tag.Get("flag")
		desc := field.Tag.Get("desc")

		var value string
		if dst && field.Tag.Get("dst") != "" {
			value = field.Tag.Get("dst")
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
			cmd.Flags().String(n, value, desc)
			_ = viper.BindPFlag(k, cmd.Flags().Lookup(n))
		case reflect.Bool:
			cmd.Flags().Bool(n, value == "true", desc)
			_ = viper.BindPFlag(k, cmd.Flags().Lookup(n))
		case reflect.Int:
			if strings.Contains(value, ":") {
				flag := util.NewRangeIntFlag(0, 0)
				if err := flag.Set(value); err != nil {
					return err
				}

				cmd.Flags().Var(flag, n, desc)
				_ = viper.BindPFlag(k, cmd.Flags().Lookup(n))
			} else {
				v, _ := strconv.Atoi(value)
				cmd.Flags().Int(n, v, desc)
				_ = viper.BindPFlag(k, cmd.Flags().Lookup(n))
			}
		case reflect.Int64:
			if field.Type == reflect.TypeOf(time.Duration(0)) {
				if strings.Contains(value, ":") {
					flag := util.NewRangeDurationFlag(0, 0)
					if err := flag.Set(value); err != nil {
						return err
					}

					cmd.Flags().Var(flag, n, desc)
					_ = viper.BindPFlag(k, cmd.Flags().Lookup(n))
				} else {
					v, _ := time.ParseDuration(value)
					cmd.Flags().Duration(n, v, desc)
					_ = viper.BindPFlag(k, cmd.Flags().Lookup(n))
				}
			} else {
				if strings.Contains(value, ":") {
					flag := util.NewRangeInt64Flag(0, 0)
					if err := flag.Set(value); err != nil {
						return err
					}

					cmd.Flags().Var(flag, n, desc)
					_ = viper.BindPFlag(k, cmd.Flags().Lookup(n))
				} else {
					v, _ := strconv.ParseInt(value, 10, 64)
					cmd.Flags().Int64(n, v, desc)
					_ = viper.BindPFlag(k, cmd.Flags().Lookup(n))
				}
			}
		case reflect.Float64:
			if strings.Contains(value, ":") {
				flag := util.NewRangeFloat64Flag(0, 0)
				if err := flag.Set(value); err != nil {
					return err
				}

				cmd.Flags().Var(flag, n, desc)
				_ = viper.BindPFlag(k, cmd.Flags().Lookup(n))
			} else {
				v, _ := strconv.ParseFloat(value, 64)
				cmd.Flags().Float64(n, v, desc)
				_ = viper.BindPFlag(k, cmd.Flags().Lookup(n))
			}
		case reflect.Slice:
			// TODO: support slice types
			// for now, slice types may only be defined in the config file
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
			cmd.Flags().StringToString(n, v, desc)
			_ = viper.BindPFlag(k, cmd.Flags().Lookup(n))
		case reflect.Struct:
			if err := bind(cmd, v.Field(i).Addr().Interface(), dst, n, k); err != nil {
				return err
			}
		default:
			panic(fmt.Sprintf("unsupported type %s", field.Type.Kind()))
		}
	}

	return nil
}

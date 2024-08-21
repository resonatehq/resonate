package config

import (
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/resonatehq/resonate/cmd/util"
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/echo"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queue"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/postgres"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/sqlite"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/grpc"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/http"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type Config config[APISubsystems, AIOSubsystems]
type ConfigDST config[APIDSTSubsystems, AIODSTSubsystems]

type config[T, U any] struct {
	System      system.Config `flag:"system"`
	API         API[T]        `flag:"api"`
	AIO         AIO[U]        `flag:"aio"`
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
	if err := viper.Unmarshal(&c); err != nil {
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
		mapstructure.StringToTimeDurationHookFunc(),
		mapstructure.StringToSliceHookFunc(","),
		mapstructure.TextUnmarshallerHookFunc(),
	)

	config := struct{ DST *ConfigDST }{DST: c}
	if err := viper.Unmarshal(&config, viper.DecodeHook(hooks)); err != nil {
		return err
	}

	return nil
}

type API[T any] struct {
	Size       int `flag:"size" desc:"submission buffered channel size" default:"1000" dst:"1:1000"`
	Subsystems T   `flag:"subsystems"`
}

type AIO[T any] struct {
	Size       int `flag:"size" desc:"completion buffered channel size" default:"1000" dst:"1:1000"`
	Subsystems T   `flag:"subsystems"`
}

type EnabledSubsystem[T any] struct {
	Enabled bool `flag:"enable" desc:"enable subsystem" default:"true"`
	Config  T    `flag:"-"`
}

type DisabledSubsystem[T any] struct {
	Enabled bool `flag:"enable" desc:"enable subsystem" default:"false"`
	Config  T    `flag:"-"`
}

type APISubsystems struct {
	Http EnabledSubsystem[http.Config] `flag:"http"`
	Grpc EnabledSubsystem[grpc.Config] `flag:"grpc"`
}

type AIOSubsystems struct {
	Echo          DisabledSubsystem[echo.Config]     `flag:"echo"`
	Queue         EnabledSubsystem[queue.Config]     `flag:"queue"`
	StorePostgres DisabledSubsystem[postgres.Config] `flag:"store-postgres"`
	StoreSqlite   EnabledSubsystem[sqlite.Config]    `flag:"store-sqlite"`
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

func (s *AIOSubsystems) Instantiate(cq chan *bus.CQE[t_aio.Submission, t_aio.Completion]) ([]aio.Subsystem, error) {
	subsystems := []aio.Subsystem{}
	if s.Echo.Enabled {
		subsystem, err := echo.New(cq, &s.Echo.Config)
		if err != nil {
			return nil, err
		}

		subsystems = append(subsystems, subsystem)
	}
	if s.Queue.Enabled {
		subsystem, err := queue.New(cq, &s.Queue.Config)
		if err != nil {
			return nil, err
		}

		subsystems = append(subsystems, subsystem)
	}

	subsystem, err := s.instantiateStore(cq)
	if err != nil {
		return nil, err
	}

	subsystems = append(subsystems, subsystem)
	return subsystems, nil
}

func (s *AIOSubsystems) instantiateStore(cq chan *bus.CQE[t_aio.Submission, t_aio.Completion]) (aio.Subsystem, error) {
	if s.StorePostgres.Enabled {
		return postgres.New(cq, &s.StorePostgres.Config)
	} else if s.StoreSqlite.Enabled {
		return sqlite.New(cq, &s.StoreSqlite.Config)
	}
	return nil, fmt.Errorf("no store enabled")
}

type APIDSTSubsystems struct {
	Http DisabledSubsystem[http.Config] `flag:"http"`
	Grpc DisabledSubsystem[grpc.Config] `flag:"grpc"`
}

type AIODSTSubsystems struct {
	Queue         EnabledSubsystem[queue.ConfigDST]  `flag:"queue"`
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

func (s *AIODSTSubsystems) Instantiate(backchannel chan interface{}) ([]aio.Subsystem, error) {
	subsystems := []aio.Subsystem{}
	if s.Queue.Enabled {
		subsystem, err := queue.NewDST(backchannel, &s.Queue.Config)
		if err != nil {
			return nil, err
		}

		subsystems = append(subsystems, subsystem)
	}

	subsystem, err := s.instantiateStore()
	if err != nil {
		return nil, err
	}

	subsystems = append(subsystems, subsystem)
	return subsystems, nil
}

func (s *AIODSTSubsystems) instantiateStore() (aio.Subsystem, error) {
	if s.StorePostgres.Enabled {
		return postgres.New(nil, &s.StorePostgres.Config)
	} else if s.StoreSqlite.Enabled {
		return sqlite.New(nil, &s.StoreSqlite.Config)
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
		case reflect.String:
			cmd.Flags().String(n, value, desc)
			_ = viper.BindPFlag(k, cmd.Flags().Lookup(n))
		case reflect.Bool:
			cmd.Flags().Bool(n, value == "true", desc)
			_ = viper.BindPFlag(k, cmd.Flags().Lookup(n))
		case reflect.Map:
			if field.Type != reflect.TypeOf(map[string]string{}) {
				panic(fmt.Sprintf("unsupported map type: %s", field.Type.Kind()))
			}
			cmd.Flags().StringToString(n, map[string]string{}, desc)
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

package util

import (
	"fmt"
	"math/rand" // nosemgrep

	"github.com/mitchellh/mapstructure"
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/network"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/postgres"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/sqlite"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/grpc"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/http"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/spf13/viper"
)

// Config

type Config struct {
	API     *APIConfig
	AIO     *AIOConfig
	System  *system.Config
	Metrics *MetricsConfig
	Log     *LogConfig
}

type APIConfig struct {
	Size       int
	Subsystems *APISubsystems
}

type AIOConfig struct {
	Size       int
	Subsystems *AIOSubsystems
}

type APISubsystems struct {
	Http *http.Config
	Grpc *grpc.Config
}

type AIOSubsystems struct {
	Store      *AIOSubsystemConfig[StoreConfig]
	Network    *AIOSubsystemConfig[network.Config]
	NetworkDST *AIOSubsystemConfig[network.ConfigDST]
	Queuing    *AIOSubsystemConfig[queuing.Config]
}

type AIOSubsystemConfig[T any] struct {
	Subsystem *aio.SubsystemConfig
	Config    *T
}

type MetricsConfig struct {
	Port int
}

type LogConfig struct {
	Level string
}

// DST Config

type ConfigDST struct {
	DST configDST
}

func (c *ConfigDST) Resolve(r *rand.Rand) *Config {
	return &Config{
		API:    c.DST.API.Resolve(r),
		AIO:    c.DST.AIO.Resolve(r),
		System: c.DST.System.Resolve(r),
		Log:    c.DST.Log,
	}
}

type configDST struct {
	API    *APIConfigDST
	AIO    *AIOConfigDST
	System *SystemConfigDST
	Log    *LogConfig
}

type APIConfigDST struct {
	Size *RangeIntFlag
}

func (c *APIConfigDST) Resolve(r *rand.Rand) *APIConfig {
	return &APIConfig{
		Size: c.Size.Resolve(r),
	}
}

type AIOConfigDST struct {
	Size       *RangeIntFlag
	Subsystems *AIOSubsystems
}

func (c *AIOConfigDST) Resolve(r *rand.Rand) *AIOConfig {
	return &AIOConfig{
		Size:       c.Size.Resolve(r),
		Subsystems: c.Subsystems,
	}
}

type SystemConfigDST struct {
	TimeoutCacheSize      *RangeIntFlag
	NotificationCacheSize *RangeIntFlag
	SubmissionBatchSize   *RangeIntFlag
	CompletionBatchSize   *RangeIntFlag
}

func (c *SystemConfigDST) Resolve(r *rand.Rand) *system.Config {
	return &system.Config{
		NotificationCacheSize: c.NotificationCacheSize.Resolve(r),
		SubmissionBatchSize:   c.SubmissionBatchSize.Resolve(r),
		CompletionBatchSize:   c.CompletionBatchSize.Resolve(r),
	}
}

// Store

type StoreKind string

const (
	Sqlite   StoreKind = "sqlite"
	Postgres StoreKind = "postgres"
)

type StoreConfig struct {
	Kind     StoreKind
	Sqlite   *sqlite.Config
	Postgres *postgres.Config
}

func NewConfig() (*Config, error) {
	var config *Config

	if err := viper.Unmarshal(&config); err != nil {
		return nil, err
	}

	return config, nil
}

func NewConfigDST(r *rand.Rand) (*Config, error) {
	var config *ConfigDST

	decodeHooks := mapstructure.ComposeDecodeHookFunc(
		mapstructure.StringToTimeDurationHookFunc(),
		mapstructure.StringToSliceHookFunc(","),
		mapstructure.TextUnmarshallerHookFunc(),
	)

	if err := viper.Unmarshal(&config, viper.DecodeHook(decodeHooks)); err != nil {
		return nil, err
	}

	return config.Resolve(r), nil
}

func NewStore(config *AIOSubsystemConfig[StoreConfig]) (aio.Subsystem, error) {
	switch config.Config.Kind {
	case Sqlite:
		return sqlite.New(config.Config.Sqlite)
	case Postgres:
		return postgres.New(config.Config.Postgres, config.Subsystem.Workers)
	default:
		return nil, fmt.Errorf("unsupported store '%s'", config.Config.Kind)
	}
}

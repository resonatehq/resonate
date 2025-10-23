package migrate

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/go-viper/mapstructure/v2"
	"github.com/resonatehq/resonate/cmd/util"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Testing approach:
//
// 1. Flag binding tests use NewCmd() directly and verify flag values
//    - This tests that flags are properly defined with correct defaults
//    - This simulates: ./resonate migrate --flag=value
//
// 2. Config file tests replicate the PersistentPreRunE logic
//    - We can't access the internal config struct from NewCmd()
//    - So we replicate: read config file -> unmarshal -> verify
//    - This simulates: ./resonate migrate --config=file.yaml
//
// 3. Override tests verify that flags take precedence over config files
//    - Uses bind + parse flags + unmarshal approach
//    - This simulates: ./resonate migrate --config=file.yaml --flag=override

// unmarshalConfig unmarshals viper config into MigrateConfig using the same hooks as the actual command
func unmarshalConfig(v *viper.Viper) (*MigrateConfig, error) {
	hooks := mapstructure.ComposeDecodeHookFunc(
		util.MapToBytes(),
		mapstructure.StringToTimeDurationHookFunc(),
		mapstructure.StringToSliceHookFunc(","),
	)

	config := &MigrateConfig{}
	if err := v.Unmarshal(&config, viper.DecodeHook(hooks)); err != nil {
		return nil, err
	}
	return config, nil
}

func TestMigrateCmd_FlagBinding(t *testing.T) {
	tests := []struct {
		name                string
		args                []string
		wantSqliteEnabled   bool
		wantPostgresEnabled bool
		wantSqlitePath      string
		wantPostgresHost    string
		wantPostgresPort    string
		wantPostgresDB      string
	}{
		{
			name:                "DefaultConfig",
			args:                []string{},
			wantSqliteEnabled:   true,
			wantPostgresEnabled: false,
			wantSqlitePath:      "resonate.db",
		},
		{
			name:                "SqliteWithCustomPath",
			args:                []string{"--aio-store-sqlite-path", "custom.db"},
			wantSqliteEnabled:   true,
			wantPostgresEnabled: false,
			wantSqlitePath:      "custom.db",
		},
		{
			name: "PostgresEnabled",
			args: []string{
				"--aio-store-postgres-enable",
				"--aio-store-postgres-host", "pghost",
				"--aio-store-postgres-port", "5433",
				"--aio-store-postgres-database", "testdb",
			},
			wantSqliteEnabled:   true,
			wantPostgresEnabled: true,
			wantPostgresHost:    "pghost",
			wantPostgresPort:    "5433",
			wantPostgresDB:      "testdb",
		},
		{
			name: "SqliteDisabled",
			args: []string{
				"--aio-store-sqlite-enable=false",
				"--aio-store-postgres-enable",
			},
			wantSqliteEnabled:   false,
			wantPostgresEnabled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := NewCmd()

			err := cmd.ParseFlags(tt.args)
			require.NoError(t, err)

			sqliteEnabled, err := cmd.PersistentFlags().GetBool("aio-store-sqlite-enable")
			require.NoError(t, err)
			assert.Equal(t, tt.wantSqliteEnabled, sqliteEnabled)

			postgresEnabled, err := cmd.PersistentFlags().GetBool("aio-store-postgres-enable")
			require.NoError(t, err)
			assert.Equal(t, tt.wantPostgresEnabled, postgresEnabled)

			if tt.wantSqlitePath != "" {
				sqlitePath, err := cmd.PersistentFlags().GetString("aio-store-sqlite-path")
				require.NoError(t, err)
				assert.Equal(t, tt.wantSqlitePath, sqlitePath)
			}

			if tt.wantPostgresHost != "" {
				pgHost, err := cmd.PersistentFlags().GetString("aio-store-postgres-host")
				require.NoError(t, err)
				assert.Equal(t, tt.wantPostgresHost, pgHost)
			}

			if tt.wantPostgresPort != "" {
				pgPort, err := cmd.PersistentFlags().GetString("aio-store-postgres-port")
				require.NoError(t, err)
				assert.Equal(t, tt.wantPostgresPort, pgPort)
			}

			if tt.wantPostgresDB != "" {
				pgDB, err := cmd.PersistentFlags().GetString("aio-store-postgres-database")
				require.NoError(t, err)
				assert.Equal(t, tt.wantPostgresDB, pgDB)
			}
		})
	}
}

func TestMigrateCmd_ConfigFile(t *testing.T) {
	tests := []struct {
		name                 string
		configContent        string
		wantSqliteEnabled    bool
		wantPostgresEnabled  bool
		wantSqlitePath       string
		wantPostgresHost     string
		wantPostgresPort     string
		wantPostgresUsername string
		wantPostgresDB       string
	}{
		{
			name: "SqliteConfig",
			configContent: `
aio:
  subsystems:
    storeSqlite:
      enabled: true
      config:
        path: "/tmp/test.db"
`,
			wantSqliteEnabled:   true,
			wantPostgresEnabled: false,
			wantSqlitePath:      "/tmp/test.db",
		},
		{
			name: "PostgresConfig",
			configContent: `
aio:
  subsystems:
    storePostgress:
      enabled: true
      config:
        host: "testhost"
        port: "5432"
        username: "testuser"
        password: "testpass"
        database: "testdb"
        query:
          sslmode: "disable"
    storeSqlite:
      enabled: false
`,
			wantSqliteEnabled:    false,
			wantPostgresEnabled:  true,
			wantPostgresHost:     "testhost",
			wantPostgresPort:     "5432",
			wantPostgresUsername: "testuser",
			wantPostgresDB:       "testdb",
		},
		{
			name: "BothStoresEnabled",
			configContent: `
aio:
  subsystems:
    storePostgress:
      enabled: true
      config:
        host: "pghost"
        port: "5433"
        database: "pgdb"
    storeSqlite:
      enabled: true
      config:
        path: "both.db"
`,
			wantSqliteEnabled:   true,
			wantPostgresEnabled: true,
			wantSqlitePath:      "both.db",
			wantPostgresHost:    "pghost",
			wantPostgresPort:    "5433",
			wantPostgresDB:      "pgdb",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temporary config file
			tmpDir := t.TempDir()
			configFile := filepath.Join(tmpDir, "resonate.yaml")
			err := os.WriteFile(configFile, []byte(tt.configContent), 0644)
			require.NoError(t, err)

			// This test simulates loading a config file the same way PersistentPreRunE does
			// We can't directly test the internal config struct from NewCmd(), so we replicate
			// the config loading logic to verify it works correctly
			v := viper.New()
			v.SetConfigFile(configFile)
			err = v.ReadInConfig()
			require.NoError(t, err)

			config, err := unmarshalConfig(v)
			require.NoError(t, err)

			// Verify the configuration was loaded correctly
			assert.Equal(t, tt.wantSqliteEnabled, config.AIO.Subsystems.StoreSqlite.Enabled, "sqlite enabled")
			assert.Equal(t, tt.wantPostgresEnabled, config.AIO.Subsystems.StorePostgress.Enabled, "postgres enabled")

			if tt.wantSqlitePath != "" {
				assert.Equal(t, tt.wantSqlitePath, config.AIO.Subsystems.StoreSqlite.Config.Path, "sqlite path")
			}

			if tt.wantPostgresHost != "" {
				assert.Equal(t, tt.wantPostgresHost, config.AIO.Subsystems.StorePostgress.Config.Host, "postgres host")
			}

			if tt.wantPostgresPort != "" {
				assert.Equal(t, tt.wantPostgresPort, config.AIO.Subsystems.StorePostgress.Config.Port, "postgres port")
			}

			if tt.wantPostgresUsername != "" {
				assert.Equal(t, tt.wantPostgresUsername, config.AIO.Subsystems.StorePostgress.Config.Username, "postgres username")
			}

			if tt.wantPostgresDB != "" {
				assert.Equal(t, tt.wantPostgresDB, config.AIO.Subsystems.StorePostgress.Config.Database, "postgres database")
			}
		})
	}
}

func TestMigrateCmd_SubcommandFlags(t *testing.T) {
	tests := []struct {
		name        string
		subcommand  string
		expectError bool
	}{
		{
			name:        "StatusSubcommand",
			subcommand:  "status",
			expectError: false,
		},
		{
			name:        "DryRunSubcommand",
			subcommand:  "dry-run",
			expectError: false,
		},
		{
			name:        "UpSubcommand",
			subcommand:  "up",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := NewCmd()

			// Find the subcommand
			var subCmd *cobra.Command
			for _, c := range cmd.Commands() {
				if c.Name() == tt.subcommand {
					subCmd = c
					break
				}
			}

			require.NotNil(t, subCmd, "subcommand %s should exist", tt.subcommand)

			// Verify that the subcommand has access to persistent flags
			configFlag := cmd.PersistentFlags().Lookup("config")
			assert.NotNil(t, configFlag, "config flag should be available")

			// Verify that sqlite and postgres flags are available
			sqliteEnableFlag := cmd.PersistentFlags().Lookup("aio-store-sqlite-enable")
			assert.NotNil(t, sqliteEnableFlag, "sqlite enable flag should be available")

			postgresEnableFlag := cmd.PersistentFlags().Lookup("aio-store-postgres-enable")
			assert.NotNil(t, postgresEnableFlag, "postgres enable flag should be available")
		})
	}
}

func TestMigrateCmd_FlagsOverrideConfigFile(t *testing.T) {
	// Create temporary config file with sqlite enabled
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "resonate.yaml")
	configContent := `
aio:
  subsystems:
    storeSqlite:
      enabled: true
      config:
        path: "config.db"
`
	err := os.WriteFile(configFile, []byte(configContent), 0644)
	require.NoError(t, err)

	// This test replicates how PersistentPreRunE works: load config, bind flags, parse args
	// Then verify flags override config file values
	v := viper.New()
	v.SetConfigFile(configFile)
	err = v.ReadInConfig()
	require.NoError(t, err)

	config := &MigrateConfig{}
	cmd := &cobra.Command{Use: "migrate"}

	// Bind config to set up flags and viper bindings
	err = config.Bind(cmd, v)
	require.NoError(t, err)

	// Parse flag that should override config
	err = cmd.ParseFlags([]string{"--aio-store-sqlite-path", "override.db"})
	require.NoError(t, err)

	// Unmarshal - viper will use flag value over config file value
	config, err = unmarshalConfig(v)
	require.NoError(t, err)

	// Verify that the flag overrode the config file value
	assert.Equal(t, "override.db", config.AIO.Subsystems.StoreSqlite.Config.Path)
}

func TestMigrateCmd_DefaultValues(t *testing.T) {
	cmd := NewCmd()

	// Don't set any flags, just verify defaults
	sqliteEnabled, err := cmd.PersistentFlags().GetBool("aio-store-sqlite-enable")
	require.NoError(t, err)
	assert.True(t, sqliteEnabled, "sqlite should be enabled by default")

	postgresEnabled, err := cmd.PersistentFlags().GetBool("aio-store-postgres-enable")
	require.NoError(t, err)
	assert.False(t, postgresEnabled, "postgres should be disabled by default")

	sqlitePath, err := cmd.PersistentFlags().GetString("aio-store-sqlite-path")
	require.NoError(t, err)
	assert.Equal(t, "resonate.db", sqlitePath, "default sqlite path")

	pgHost, err := cmd.PersistentFlags().GetString("aio-store-postgres-host")
	require.NoError(t, err)
	assert.Equal(t, "localhost", pgHost, "default postgres host")

	pgPort, err := cmd.PersistentFlags().GetString("aio-store-postgres-port")
	require.NoError(t, err)
	assert.Equal(t, "5432", pgPort, "default postgres port")

	pgDB, err := cmd.PersistentFlags().GetString("aio-store-postgres-database")
	require.NoError(t, err)
	assert.Equal(t, "resonate", pgDB, "default postgres database")
}

func TestMigrateCmd_PostgresQueryParams(t *testing.T) {
	// Create config file with custom query params
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "resonate.yaml")
	configContent := `
aio:
  subsystems:
    storePostgress:
      enabled: true
      config:
        host: "localhost"
        port: "5432"
        database: "testdb"
        query:
          sslmode: "require"
          connect_timeout: "10"
`
	err := os.WriteFile(configFile, []byte(configContent), 0644)
	require.NoError(t, err)

	v := viper.New()
	v.SetConfigFile(configFile)
	err = v.ReadInConfig()
	require.NoError(t, err)

	config, err := unmarshalConfig(v)
	require.NoError(t, err)

	// Verify query params
	assert.Equal(t, "require", config.AIO.Subsystems.StorePostgress.Config.Query["sslmode"])
	assert.Equal(t, "10", config.AIO.Subsystems.StorePostgress.Config.Query["connect_timeout"])
}

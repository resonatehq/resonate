package migrate

import (
	"fmt"
	"strings"

	"github.com/go-viper/mapstructure/v2"
	"github.com/resonatehq/resonate/cmd/config"
	"github.com/resonatehq/resonate/cmd/util"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/migrations"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/postgres"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/sqlite"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

// MigrateConfig holds configuration for the migrate command
type MigrateConfig struct {
	Store MigrateStoreConfig `flag:"aio-store"`
}

// MigrateStoreConfig holds store-specific configuration
type MigrateStoreConfig struct {
	Postgres PostgresMigrateConfig `flag:"postgres"`
	Sqlite   SqliteMigrateConfig   `flag:"sqlite"`
}

// PostgresMigrateConfig holds postgres-specific configuration
type PostgresMigrateConfig struct {
	Enabled  bool              `flag:"enable" desc:"enable postgres store" default:"false"`
	Host     string            `flag:"host" desc:"postgres host" default:"localhost"`
	Port     string            `flag:"port" desc:"postgres port" default:"5432"`
	Username string            `flag:"username" desc:"postgres username"`
	Password string            `flag:"password" desc:"postgres password"`
	Database string            `flag:"database" desc:"postgres database" default:"resonate"`
	Query    map[string]string `flag:"query" desc:"postgres query options" dst:"{\"sslmode\":\"disable\"}" dev:"{\"sslmode\":\"disable\"}"`
}

// SqliteMigrateConfig holds sqlite-specific configuration
type SqliteMigrateConfig struct {
	Enabled bool   `flag:"enable" desc:"enable sqlite store" default:"true"`
	Path    string `flag:"path" desc:"sqlite database path" default:"resonate.db"`
}

func (c *MigrateConfig) Bind(cmd *cobra.Command, vip *viper.Viper) error {
	return config.Bind(cmd, c, vip, "default", "", "")
}

func NewCmd() *cobra.Command {
	var (
		v      = viper.New()
		config = &MigrateConfig{}
	)

	migrateCmd := &cobra.Command{
		Use:   "migrate",
		Short: "Database migration commands",
		Long:  "Manage database migrations for Resonate",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if file, _ := cmd.Flags().GetString("config"); file != "" {
				v.SetConfigFile(file)
			} else {
				v.SetConfigName("resonate")
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

			// Unmarshal config
			hooks := mapstructure.ComposeDecodeHookFunc(
				util.MapToBytes(),
				mapstructure.StringToTimeDurationHookFunc(),
				mapstructure.StringToSliceHookFunc(","),
			)

			if err := v.Unmarshal(&config, viper.DecodeHook(hooks)); err != nil {
				return err
			}

			return nil
		},
	}

	// bind config file flag
	migrateCmd.PersistentFlags().StringP("config", "c", "", "config file (default resonate.yaml)")

	// bind config
	_ = config.Bind(migrateCmd, v)

	// Add subcommands
	migrateCmd.AddCommand(newStatusCmd(config))
	migrateCmd.AddCommand(newDryRunCmd(config))
	migrateCmd.AddCommand(newUpCmd(config))

	return migrateCmd
}

func newStatusCmd(config *MigrateConfig) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show current migration status",
		RunE: func(cmd *cobra.Command, args []string) error {
			store, err := getMigrationStore(config)
			if err != nil {
				return err
			}
			defer store.Close()

			currentVersion, err := store.GetCurrentVersion()
			if err != nil {
				return fmt.Errorf("failed to get current version: %w", err)
			}

			allMigrations, err := migrations.LoadMigrations(store)
			if err != nil {
				return fmt.Errorf("failed to load migrations: %w", err)
			}

			var latestVersion int
			if len(allMigrations) > 0 {
				latestVersion = allMigrations[len(allMigrations)-1].Version
			} else {
				latestVersion = currentVersion
			}

			pending, err := migrations.GetPendingMigrations(currentVersion, store)
			if err != nil {
				return fmt.Errorf("failed to get pending migrations: %w", err)
			}

			cmd.Printf("Store type: %s\n", store.String())
			cmd.Printf("Current migration version: %d\n", currentVersion)
			cmd.Printf("Latest migration version: %d\n", latestVersion)
			cmd.Printf("Pending migrations: %d\n", len(pending))

			if len(pending) > 0 {
				cmd.Println("\nPending migration files:")
				for _, m := range pending {
					cmd.Printf("  %03d_%s.sql\n", m.Version, m.Name)
				}
				cmd.Println("\nStatus: MIGRATIONS PENDING")
			} else {
				cmd.Println("\nStatus: UP TO DATE")
			}

			return nil
		},
	}
}

func newDryRunCmd(config *MigrateConfig) *cobra.Command {
	return &cobra.Command{
		Use:   "dry-run",
		Short: "Show which migrations would be applied",
		RunE: func(cmd *cobra.Command, args []string) error {
			store, err := getMigrationStore(config)
			if err != nil {
				return err
			}
			defer store.Close()

			currentVersion, err := store.GetCurrentVersion()
			if err != nil {
				return fmt.Errorf("failed to get current version: %w", err)
			}

			pending, err := migrations.GetPendingMigrations(currentVersion, store)
			if err != nil {
				return fmt.Errorf("failed to get pending migrations: %w", err)
			}

			if len(pending) == 0 {
				cmd.Println("No pending migrations")
				return nil
			}

			cmd.Printf("Would apply the following %d migration(s):\n\n", len(pending))
			for _, m := range pending {
				cmd.Printf("%03d_%s.sql\n", m.Version, m.Name)
				cmd.Printf("%s\n\n", m.SQL)
			}

			return nil
		},
	}
}

func newUpCmd(config *MigrateConfig) *cobra.Command {
	return &cobra.Command{
		Use:   "up",
		Short: "Apply all pending migrations",
		RunE: func(cmd *cobra.Command, args []string) error {
			store, err := getMigrationStore(config)
			if err != nil {
				return err
			}
			defer store.Close()

			currentVersion, err := store.GetCurrentVersion()
			if err != nil {
				return fmt.Errorf("failed to get current version: %w", err)
			}

			pending, err := migrations.GetPendingMigrations(currentVersion, store)
			if err != nil {
				return fmt.Errorf("failed to get pending migrations: %w", err)
			}

			if len(pending) == 0 {
				cmd.Println("No pending migrations")
				return nil
			}

			// Validate migration sequence
			if err := migrations.ValidateMigrationSequence(pending, currentVersion); err != nil {
				return err
			}

			cmd.Printf("Applying %d migration(s)...\n\n", len(pending))

			if err := migrations.ApplyMigrations(pending, store, true); err != nil {
				return err
			}

			latestVersion := pending[len(pending)-1].Version
			cmd.Printf("\n✓ Successfully applied %d migration(s). Database is now at version %d.\n",
				len(pending), latestVersion)

			return nil
		},
	}
}

// getMigrationStore infers the store type, opens the database connection,
// and returns the appropriate migration store. The store owns the database
// connection and the caller is responsible for calling Close() on the store.
func getMigrationStore(config *MigrateConfig) (migrations.MigrationStore, error) {
	// Infer store type from enable flags
	sqliteEnabled := config.Store.Sqlite.Enabled
	postgresEnabled := config.Store.Postgres.Enabled

	if !postgresEnabled && !sqliteEnabled {
		return nil, fmt.Errorf("no store enabled; enable either sqlite or postgres")
	}

	if postgresEnabled {
		pgConfig := config.Store.Postgres

		db, err := postgres.NewConn(&postgres.ConnConfig{
			Host:     pgConfig.Host,
			Port:     pgConfig.Port,
			Username: pgConfig.Username,
			Password: pgConfig.Password,
			Database: pgConfig.Database,
			Query:    pgConfig.Query,
		})
		if err != nil {
			return nil, err
		}
		return migrations.NewPostgresMigrationStore(db), nil
	} else {
		db, err := sqlite.NewConn(config.Store.Sqlite.Path)
		if err != nil {
			return nil, err
		}
		return migrations.NewSqliteMigrationStore(db), nil
	}
}

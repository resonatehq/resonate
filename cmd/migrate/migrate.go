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
	internalUtil "github.com/resonatehq/resonate/internal/util"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

func NewCmd(cfg *config.Config, vip *viper.Viper) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Database migration commands",
		Long:  "Manage database migrations for Resonate",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if file, _ := cmd.PersistentFlags().GetString("config"); file != "" {
				vip.SetConfigFile(file)
			} else {
				vip.SetConfigName("resonate")
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

			hooks := mapstructure.ComposeDecodeHookFunc(
				util.MapToBytes(),
				mapstructure.StringToTimeDurationHookFunc(),
				mapstructure.StringToSliceHookFunc(","),
			)

			// decode subsystems
			for _, subsystem := range cfg.AIO.Subsystems.All() {
				if err := subsystem.Decode(vip, hooks); err != nil {
					return err
				}
			}

			return nil
		},
	}

	// bind config file flag
	cmd.PersistentFlags().StringP("config", "c", "", "config file (default resonate.yaml)")

	// bind plugins
	for _, plugin := range cfg.Plugins() {
		plugin.Bind(cmd, cmd.PersistentFlags(), vip, cmd.Name())
	}

	// add subcommands
	cmd.AddCommand(newStatusCmd(cfg))
	cmd.AddCommand(newDryRunCmd(cfg))
	cmd.AddCommand(newUpCmd(cfg))

	return cmd
}

func newStatusCmd(cfg *config.Config) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show current migration status",
		RunE: func(cmd *cobra.Command, args []string) error {
			store, err := getMigrationStore(cfg)
			if err != nil {
				return err
			}
			defer internalUtil.DeferAndLog(store.Close)

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

func newDryRunCmd(cfg *config.Config) *cobra.Command {
	return &cobra.Command{
		Use:   "dry-run",
		Short: "Show which migrations would be applied",
		RunE: func(cmd *cobra.Command, args []string) error {
			store, err := getMigrationStore(cfg)
			if err != nil {
				return err
			}
			defer internalUtil.DeferAndLog(store.Close)

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

func newUpCmd(cfg *config.Config) *cobra.Command {
	return &cobra.Command{
		Use:   "up",
		Short: "Apply all pending migrations",
		RunE: func(cmd *cobra.Command, args []string) error {
			store, err := getMigrationStore(cfg)
			if err != nil {
				return err
			}
			defer internalUtil.DeferAndLog(store.Close)

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

			if err := migrations.ValidateMigrationSequence(pending, currentVersion); err != nil {
				return err
			}

			cmd.Printf("Applying %d migration(s)...\n\n", len(pending))

			if err := migrations.ApplyMigrations(pending, store); err != nil {
				return err
			}

			for _, migration := range pending {
				cmd.Printf("✓  %03d_%s.sql\n", migration.Version, migration.Name)
			}

			latestVersion := pending[len(pending)-1].Version
			cmd.Printf("\nSuccessfully applied %d migration(s). Database is now at version %d.\n",
				len(pending), latestVersion)

			return nil
		},
	}
}

// getMigrationStore infers the store type, opens the database connection,
// and returns the appropriate migration store. The store owns the database
// connection and the caller is responsible for calling Close() on the store.
func getMigrationStore(cfg *config.Config) (migrations.MigrationStore, error) {
	for _, plugin := range cfg.AIO.Subsystems.All() {
		if plugin.Name() == "store-postgres" && plugin.Enabled() {
			subsystem, err := plugin.New(nil, nil)
			if err != nil {
				return nil, err
			}

			store := subsystem.(*postgres.PostgresStore)
			return migrations.NewPostgresMigrationStore(store.DB()), nil
		}
		if plugin.Name() == "store-sqlite" && plugin.Enabled() {
			subsystem, err := plugin.New(nil, nil)
			if err != nil {
				return nil, err
			}

			store := subsystem.(*sqlite.SqliteStore)
			return migrations.NewSqliteMigrationStore(store.DB()), nil
		}
	}

	return nil, fmt.Errorf("no enabled aio store plugin found for migrations")
}

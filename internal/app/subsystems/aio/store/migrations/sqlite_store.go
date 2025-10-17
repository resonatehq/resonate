package migrations

import (
	"database/sql"
	"fmt"

	"github.com/resonatehq/resonate/internal/migrationfiles"
)

// SqliteMigrationStore implements MigrationStore for SQLite databases
type SqliteMigrationStore struct {
	db *sql.DB
}

// NewSqliteMigrationStore creates a new SQLite migration store
func NewSqliteMigrationStore(db *sql.DB) *SqliteMigrationStore {
	return &SqliteMigrationStore{db: db}
}

func (s *SqliteMigrationStore) GetMigrationFiles() ([]string, error) {
	return migrationfiles.GetSQLiteMigrationFiles()
}

func (s *SqliteMigrationStore) GetMigrationContent(path string) (string, error) {
	return migrationfiles.GetSQLiteMigrationContent(path)
}

func (s *SqliteMigrationStore) GetInsertMigrationSQL() string {
	return "INSERT INTO migrations (id) VALUES (?) ON CONFLICT(id) DO NOTHING"
}

func (s *SqliteMigrationStore) GetCurrentVersion() (int, error) {
	var version int
	err := s.db.QueryRow("SELECT id FROM migrations ORDER BY id DESC LIMIT 1").Scan(&version)

	// We create the migrations table on store start, we can assume that ErrNoRows means that no migrations have been applied
	// and the db is fresh
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("failed to get current migration version: %w", err)
	}
	return version, nil
}

func (s *SqliteMigrationStore) GetDB() *sql.DB {
	return s.db
}

func (s *SqliteMigrationStore) CheckMigrations() error {
	currentVersion, err := s.GetCurrentVersion()
	if err != nil {
		return err
	}

	pending, err := GetPendingMigrations(currentVersion, s)
	if err != nil {
		return err
	}

	if len(pending) > 0 {
		return &MigrationError{
			Version: pending[0].Version,
			Name:    pending[0].Name,
			Err:     ErrPendingMigrations,
		}
	}

	return nil
}

func (s *SqliteMigrationStore) String() string {
	return "sqlite"
}

func (s *SqliteMigrationStore) Close() error {
	return s.db.Close()
}

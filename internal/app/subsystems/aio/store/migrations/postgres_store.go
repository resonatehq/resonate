package migrations

import (
	"database/sql"
	"fmt"

	"github.com/resonatehq/resonate/internal/migrationfiles"
)

// PostgresMigrationStore implements MigrationStore for PostgreSQL databases
type PostgresMigrationStore struct {
	db *sql.DB
}

// NewPostgresMigrationStore creates a new PostgreSQL migration store
func NewPostgresMigrationStore(db *sql.DB) *PostgresMigrationStore {
	return &PostgresMigrationStore{db: db}
}

func (s *PostgresMigrationStore) GetMigrationFiles() ([]string, error) {
	return migrationfiles.GetPostgresMigrationFiles()
}

func (s *PostgresMigrationStore) GetMigrationContent(path string) (string, error) {
	return migrationfiles.GetPostgresMigrationContent(path)
}

func (s *PostgresMigrationStore) GetInsertMigrationSQL() string {
	return "INSERT INTO migrations (id) VALUES ($1) ON CONFLICT(id) DO NOTHING"
}

func (s *PostgresMigrationStore) GetCurrentVersion() (int, error) {
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

func (s *PostgresMigrationStore) GetDB() *sql.DB {
	return s.db
}

func (s *PostgresMigrationStore) CheckMigrations() error {
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

func (s *PostgresMigrationStore) String() string {
	return "postgres"
}

func (s *PostgresMigrationStore) Close() error {
	return s.db.Close()
}

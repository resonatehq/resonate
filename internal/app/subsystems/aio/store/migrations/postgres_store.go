package migrations

import (
	"database/sql"

	"github.com/resonatehq/resonate/internal/migrationfiles"
)

// PostgresMigrationStore implements MigrationStore for PostgreSQL databases
type PostgresMigrationStore struct{}

// NewPostgresMigrationStore creates a new PostgreSQL migration store
func NewPostgresMigrationStore() *PostgresMigrationStore {
	return &PostgresMigrationStore{}
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

func (s *PostgresMigrationStore) CheckMigrations(db *sql.DB) error {
	currentVersion, err := GetCurrentVersion(db)
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

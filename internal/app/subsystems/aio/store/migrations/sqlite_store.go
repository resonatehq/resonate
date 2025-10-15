package migrations

import (
	"database/sql"

	"github.com/resonatehq/resonate/internal/migrationfiles"
)

// SqliteMigrationStore implements MigrationStore for SQLite databases
type SqliteMigrationStore struct{}

// NewSqliteMigrationStore creates a new SQLite migration store
func NewSqliteMigrationStore() *SqliteMigrationStore {
	return &SqliteMigrationStore{}
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

func (s *SqliteMigrationStore) CheckMigrations(db *sql.DB) error {
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

func (s *SqliteMigrationStore) String() string {
	return "sqlite"
}

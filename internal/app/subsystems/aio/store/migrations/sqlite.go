package migrations

import "database/sql"

// CheckSQLiteMigrations checks if there are pending migrations for SQLite
func CheckSQLiteMigrations(db *sql.DB) error {
	store := NewSqliteMigrationStore(db)
	return store.CheckMigrations()
}

package migrationfiles

import (
	"embed"
	"io/fs"
	"path/filepath"
)

//go:embed migrations/sqlite migrations/postgres
var MigrationsFS embed.FS

// GetSQLiteMigrationFiles returns list of SQLite migration files
func GetSQLiteMigrationFiles() ([]string, error) {
	return getMigrationFiles(MigrationsFS, "migrations/sqlite")
}

// GetPostgresMigrationFiles returns list of Postgres migration files
func GetPostgresMigrationFiles() ([]string, error) {
	return getMigrationFiles(MigrationsFS, "migrations/postgres")
}

// GetSQLiteMigrationContent reads a SQLite migration file
func GetSQLiteMigrationContent(path string) (string, error) {
	return getMigrationContent(MigrationsFS, path)
}

// GetPostgresMigrationContent reads a Postgres migration file
func GetPostgresMigrationContent(path string) (string, error) {
	return getMigrationContent(MigrationsFS, path)
}

func getMigrationFiles(fsys embed.FS, dir string) ([]string, error) {
	var files []string
	err := fs.WalkDir(fsys, dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && filepath.Ext(path) == ".sql" {
			files = append(files, path)
		}
		return nil
	})
	return files, err
}

func getMigrationContent(fsys embed.FS, path string) (string, error) {
	content, err := fsys.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

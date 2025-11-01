package migrationfiles

import (
	"embed"
	"io/fs"
	"path/filepath"
)

//go:embed migrations/sqlite migrations/postgres
var MigrationsFS embed.FS

func GetMigrationFiles(dir string) ([]string, error) {
	var files []string
	err := fs.WalkDir(MigrationsFS, dir, func(path string, d fs.DirEntry, err error) error {
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

func GetMigrationContent(path string) (string, error) {
	content, err := MigrationsFS.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

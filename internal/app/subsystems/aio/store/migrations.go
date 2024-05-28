package store

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type migration struct {
	Version int
	Content []byte
}

func Start(db *sql.DB, txTimeout time.Duration, migrationsFS embed.FS) error {
	currentVersion, err := readVersion(db)
	if err != nil {
		return err
	}

	migrationsToApply, err := readMigrations(migrationsFS, currentVersion)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), txTimeout)
	defer cancel()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				err = fmt.Errorf("tx failed: %v, unable to rollback: %v", err, rbErr)
			}
		}
	}()

	for _, m := range migrationsToApply {
		_, err = tx.Exec(string(m.Content))
		if err != nil {
			return fmt.Errorf("failed to execute migration version %d: %w", m.Version, err)
		}
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	return nil
}

func readVersion(db *sql.DB) (int, error) {
	var version int
	err := db.QueryRow("SELECT id FROM migrations").Scan(&version)
	if err != nil {
		if isTableNotFoundError(err) {
			return -1, nil
		}
		return 0, err
	}
	return version, nil
}

// db.QueryRow does not return a specific error type when the table does
// not exist so we need to check the error message.
func isTableNotFoundError(err error) bool {
	errStr := err.Error()
	if strings.Contains(errStr, "no such table") || strings.Contains(errStr, "does not exist") {
		return true
	}
	return false
}

func readMigrations(migrationsFS embed.FS, currentVersion int) ([]migration, error) {
	migrations := []migration{}

	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		filename := entry.Name()
		content, err := migrationsFS.ReadFile("migrations/" + filename)
		if err != nil {
			return nil, err
		}

		version, err := filenameVersion(filename)
		if err != nil {
			return nil, err
		}

		// Skip migrations that are at or below the current version.
		if version <= currentVersion {
			continue
		}

		migrations = append(migrations, migration{
			Version: version,
			Content: content,
		})
	}

	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})

	return migrations, nil
}

func filenameVersion(filename string) (int, error) {
	// Use a regular expression to extract the version number
	// from the filename by matching the first sequence of digits.
	re := regexp.MustCompile(`\d+`)
	versionStr := re.FindString(filepath.Base(filename))

	if versionStr == "" {
		return 0, fmt.Errorf("could not extract version from filename: %s", filename)
	}

	version, err := strconv.Atoi(versionStr)
	if err != nil {
		return 0, fmt.Errorf("could not convert version string to int: %v", err)
	}

	return version, nil
}

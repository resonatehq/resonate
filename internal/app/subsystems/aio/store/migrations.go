package store

import (
	"database/sql"
	"embed"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type migration struct {
	Version int
	Content []byte
}

func ReadVersion(db *sql.DB) (int, error) {
	var version int
	err := db.QueryRow("SELECT id FROM migrations").Scan(&version)
	if err != nil {
		if strings.Contains(err.Error(), "no such table") {
			return -1, nil
		}
		return 0, err
	}

	return version, nil
}

func ReadMigrations(migrationsFS embed.FS, currentVersion int) ([]migration, error) {
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

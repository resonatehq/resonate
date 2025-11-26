package test

import (
	"testing"

	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/migrations"
)

func TestParseMigrationFilename(t *testing.T) {
	tests := []struct {
		name        string
		filename    string
		wantVersion int
		wantName    string
		wantErr     bool
		errContains string
	}{
		{
			name:        "valid migration file",
			filename:    "001_initial_schema.sql",
			wantVersion: 1,
			wantName:    "initial_schema",
			wantErr:     false,
		},
		{
			name:        "valid migration with underscores",
			filename:    "042_add_user_permissions.sql",
			wantVersion: 42,
			wantName:    "add_user_permissions",
			wantErr:     false,
		},
		{
			name:        "valid migration with multiple underscores",
			filename:    "100_add_index_on_user_created_at.sql",
			wantVersion: 100,
			wantName:    "add_index_on_user_created_at",
			wantErr:     false,
		},
		{
			name:        "missing extension",
			filename:    "001_initial_schema",
			wantErr:     true,
			errContains: "invalid migration filename format",
		},
		{
			name:        "wrong extension",
			filename:    "001_initial_schema.txt",
			wantErr:     true,
			errContains: "invalid migration filename format",
		},
		{
			name:        "missing version",
			filename:    "initial_schema.sql",
			wantErr:     true,
			errContains: "invalid migration filename format",
		},
		{
			name:        "invalid version format - too short",
			filename:    "01_initial_schema.sql",
			wantErr:     true,
			errContains: "invalid migration filename format",
		},
		{
			name:        "invalid version format - too long",
			filename:    "0001_initial_schema.sql",
			wantErr:     true,
			errContains: "invalid migration filename format",
		},
		{
			name:        "missing underscore separator",
			filename:    "001initial_schema.sql",
			wantErr:     true,
			errContains: "invalid migration filename format",
		},
		{
			name:        "missing name",
			filename:    "001_.sql",
			wantErr:     true,
			errContains: "invalid migration filename format",
		},
		{
			name:        "version with leading zeros",
			filename:    "007_james_bond.sql",
			wantVersion: 7,
			wantName:    "james_bond",
			wantErr:     false,
		},
		{
			name:        "non-numeric version",
			filename:    "abc_initial_schema.sql",
			wantErr:     true,
			errContains: "invalid migration filename format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			version, name, err := migrations.ParseMigrationFilename(tt.filename)

			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseMigrationFilename() expected error but got none")
					return
				}
				if tt.errContains != "" && !contains(err.Error(), tt.errContains) {
					t.Errorf("ParseMigrationFilename() error = %v, should contain %q", err, tt.errContains)
				}
				return
			}

			if err != nil {
				t.Errorf("ParseMigrationFilename() unexpected error: %v", err)
				return
			}

			if version != tt.wantVersion {
				t.Errorf("ParseMigrationFilename() version = %d, want %d", version, tt.wantVersion)
			}

			if name != tt.wantName {
				t.Errorf("ParseMigrationFilename() name = %q, want %q", name, tt.wantName)
			}
		})
	}
}

func TestValidateMigrationSequence(t *testing.T) {
	tests := []struct {
		name         string
		migrations   []migrations.Migration
		startVersion int
		wantErr      bool
		errContains  string
	}{
		{
			name: "valid sequence starting from 0",
			migrations: []migrations.Migration{
				{Version: 1, Name: "first"},
				{Version: 2, Name: "second"},
				{Version: 3, Name: "third"},
			},
			startVersion: 0,
			wantErr:      false,
		},
		{
			name: "valid sequence starting from 5",
			migrations: []migrations.Migration{
				{Version: 6, Name: "sixth"},
				{Version: 7, Name: "seventh"},
			},
			startVersion: 5,
			wantErr:      false,
		},
		{
			name: "single migration",
			migrations: []migrations.Migration{
				{Version: 1, Name: "only"},
			},
			startVersion: 0,
			wantErr:      false,
		},
		{
			name:         "empty migrations",
			migrations:   []migrations.Migration{},
			startVersion: 0,
			wantErr:      false,
		},
		{
			name: "gap in sequence - missing version 2",
			migrations: []migrations.Migration{
				{Version: 1, Name: "first"},
				{Version: 3, Name: "third"},
			},
			startVersion: 0,
			wantErr:      true,
			errContains:  "migration sequence gap: expected version 2, found 3",
		},
		{
			name: "gap at start",
			migrations: []migrations.Migration{
				{Version: 2, Name: "second"},
				{Version: 3, Name: "third"},
			},
			startVersion: 0,
			wantErr:      true,
			errContains:  "migration sequence gap: expected version 1, found 2",
		},
		{
			name: "gap in middle",
			migrations: []migrations.Migration{
				{Version: 6, Name: "sixth"},
				{Version: 7, Name: "seventh"},
				{Version: 9, Name: "ninth"},
			},
			startVersion: 5,
			wantErr:      true,
			errContains:  "migration sequence gap: expected version 8, found 9",
		},
		{
			name: "duplicate versions",
			migrations: []migrations.Migration{
				{Version: 1, Name: "first"},
				{Version: 1, Name: "first_again"},
			},
			startVersion: 0,
			wantErr:      true,
			errContains:  "migration sequence gap: expected version 2, found 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := migrations.ValidateMigrationSequence(tt.migrations, tt.startVersion)

			if tt.wantErr {
				if err == nil {
					t.Errorf("ValidateMigrationSequence() expected error but got none")
					return
				}
				if tt.errContains != "" && !contains(err.Error(), tt.errContains) {
					t.Errorf("ValidateMigrationSequence() error = %v, should contain %q", err, tt.errContains)
				}
				return
			}

			if err != nil {
				t.Errorf("ValidateMigrationSequence() unexpected error: %v", err)
			}
		})
	}
}

// Helper function to check if string contains substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		func() bool {
			for i := 0; i <= len(s)-len(substr); i++ {
				if s[i:i+len(substr)] == substr {
					return true
				}
			}
			return false
		}())
}

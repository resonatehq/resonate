package log

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseLevel(t *testing.T) {
	for _, tc := range []struct {
		name        string
		lvl         string
		parsedLevel slog.Level
		hasErr      bool
	}{
		{
			name:        "Empty string",
			lvl:         "",
			parsedLevel: 0,
			hasErr:      true,
		},
		{
			name:        "Uppercase level",
			lvl:         "DEBUG",
			parsedLevel: DebugLevel,
			hasErr:      false,
		},
		{
			name:        "Debug",
			lvl:         "debug",
			parsedLevel: DebugLevel,
			hasErr:      false,
		},
		{
			name:        "Info",
			lvl:         "info",
			parsedLevel: InfoLevel,
			hasErr:      false,
		},
		{
			name:        "Warn",
			lvl:         "warn",
			parsedLevel: WarnLevel,
			hasErr:      false,
		},
		{
			name:        "Error",
			lvl:         "error",
			parsedLevel: ErrorLevel,
			hasErr:      false,
		},
		{
			name:        "Unsupported level",
			lvl:         "XXX",
			parsedLevel: 0,
			hasErr:      true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l, err := ParseLevel(tc.lvl)

			assert.Equal(t, tc.parsedLevel, l)

			if tc.hasErr {
				assert.NotNil(t, err)
				assert.ErrorContains(t, err, "unrecognized level: ")
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

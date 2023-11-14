package log

import (
	"fmt"
	"log/slog"
	"strings"
)

const (
	// DebugLevel defines debug log level.
	DebugLevel = slog.LevelDebug
	// InfoLevel defines info log level.
	InfoLevel = slog.LevelInfo
	// WarnLevel defines warn log level.
	WarnLevel = slog.LevelWarn
	// ErrorLevel defines error log level.
	ErrorLevel = slog.LevelError
)

// ParseLevel takes a string level and returns the slog log level constant.
func ParseLevel(lvl string) (slog.Level, error) {
	switch strings.ToLower(lvl) {
	case "debug":
		return DebugLevel, nil
	case "info":
		return InfoLevel, nil
	case "warn":
		return WarnLevel, nil
	case "error":
		return ErrorLevel, nil
	default:
		return 0, fmt.Errorf("unrecognized level: %q", lvl)
	}
}

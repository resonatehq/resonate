package log

import (
	"fmt"
	"log/slog"
	"strings"
)

const (
	DebugLevel = slog.LevelDebug
	InfoLevel  = slog.LevelInfo
	WarnLevel  = slog.LevelWarn
	ErrorLevel = slog.LevelError
	OffLevel   = slog.Level(1000)
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
	case "off":
		return OffLevel, nil
	default:
		return 0, fmt.Errorf("unrecognized level: %s", lvl)
	}
}

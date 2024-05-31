package version

import (
	"fmt"
	"runtime"
	"time"
)

// current version
const (
	coreVersion = "0.5.7"
	prerelease  = "alpha"
)

// Provisioned by ldflags
var commit string

// Core return the core version.
func Core() string {
	return coreVersion
}

// Short return the version with pre-release, if available.
func Short() string {
	if prerelease != "" {
		return fmt.Sprintf("%s-%s-%s", coreVersion, prerelease, time.Now().Format("20060102"))
	}

	return coreVersion
}

// Full return the full version including pre-release, commit hash, runtime os and arch.
func Full() string {
	if commit != "" && commit[:1] != " " {
		commit = " " + commit
	}

	return fmt.Sprintf("v%s%s %s/%s", Short(), commit, runtime.GOOS, runtime.GOARCH)
}

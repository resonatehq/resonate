package version

import (
	"fmt"
	"runtime/debug"

	"github.com/spf13/cobra"
)

var Version string = "dev"

var VersionCmd = &cobra.Command{
	Use: "version",
	Run: func(cmd *cobra.Command, args []string) {
		buildInfo, buildInfoRead := debug.ReadBuildInfo()
		var commitHash = "dev"
		if buildInfoRead {
			for _, setting := range buildInfo.Settings {
				if setting.Key == "vcs.revision" {
					commitHash = setting.Value
					break
				}
			}
		}
		fmt.Println("resonate version", Version, "("+commitHash+")")
	},
}

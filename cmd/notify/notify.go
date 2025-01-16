package notify

import (
	"github.com/resonatehq/resonate/pkg/client"
	"github.com/spf13/cobra"
)

func NewCmd() *cobra.Command {
	var (
		c        = client.New()
		server   string
		username string
		password string
	)

	cmd := &cobra.Command{
		Use:     "notifications",
		Aliases: []string{"notifications"},
		Short:   "Resonate notifications",
		Run: func(cmd *cobra.Command, args []string) {
			_ = cmd.Help()
		},
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if username != "" || password != "" {
				c.SetBasicAuth(username, password)
			}

			return c.Setup(server)
		},
	}

	// Add subcommands
	cmd.AddCommand(CreateNotifyCmd(c))

	// Flags
	cmd.PersistentFlags().StringVarP(&server, "server", "", "http://localhost:8001", "resonate url")
	cmd.PersistentFlags().StringVarP(&username, "username", "U", "", "basic auth username")
	cmd.PersistentFlags().StringVarP(&password, "password", "P", "", "basic auth password")

	return cmd
}

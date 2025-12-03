package tasks

import (
	"github.com/resonatehq/resonate/pkg/client"
	"github.com/spf13/cobra"
)

// NewCmd returns the root command for the `tasks` module.
func NewCmd() *cobra.Command {
	var (
		c        = client.New()
		server   string
		username string
		password string
		token    string
	)

	cmd := &cobra.Command{
		Use:     "tasks",
		Aliases: []string{"task"},
		Short:   "Resonate tasks",
		Run: func(cmd *cobra.Command, args []string) {
			_ = cmd.Help()
		},
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if username != "" || password != "" {
				c.SetBasicAuth(username, password)
			}
			if token != "" {
				c.SetBearerToken(token)
			}

			return c.Setup(server)
		},
	}

	// Add subcommands
	cmd.AddCommand(ClaimTaskCmd(c))     // Claim task subcommand
	cmd.AddCommand(CompleteTaskCmd(c))  // Complete task subcommand
	cmd.AddCommand(HeartbeatTaskCmd(c)) // Heartbeat task subcommand

	// Flags
	cmd.PersistentFlags().StringVarP(&server, "server", "S", "http://localhost:8001", "resonate url")
	cmd.PersistentFlags().StringVarP(&token, "token", "T", "", "JWT bearer token")
	cmd.PersistentFlags().StringVarP(&username, "username", "U", "", "basic auth username")
	cmd.PersistentFlags().StringVarP(&password, "password", "P", "", "basic auth password")

	return cmd
}

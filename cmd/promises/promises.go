package promises

import (
	"fmt"
	"strings"
	"text/tabwriter"

	"github.com/resonatehq/resonate/cmd/util"
	"github.com/resonatehq/resonate/pkg/client"
	v1 "github.com/resonatehq/resonate/pkg/client/v1"
	"github.com/spf13/cobra"
)

func NewCmd() *cobra.Command {
	var (
		c        = client.New()
		server   string
		username string
		password string
		token    string
	)

	cmd := &cobra.Command{
		Use:     "promises",
		Aliases: []string{"promise"},
		Short:   "Resonate promises",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if username != "" || password != "" {
				c.SetBasicAuth(username, password)
			}

			if token != "" {
				c.SetBearerToken(token)
			}

			return c.Setup(server)
		},
		Run: func(cmd *cobra.Command, args []string) {
			_ = cmd.Help()
		},
	}

	// Add subcommands
	cmd.AddCommand(GetPromiseCmd(c))
	cmd.AddCommand(SearchPromisesCmd(c))
	cmd.AddCommand(CreatePromiseCmd(c))
	cmd.AddCommand(CompletePromiseCmds(c)...)
	cmd.AddCommand(CreatePromiseCallbackCmd(c))
	cmd.AddCommand(CreateSubscriptionCmd(c))

	// Flags
	cmd.PersistentFlags().StringVarP(&server, "server", "", "http://localhost:8001", "resonate url")
	cmd.PersistentFlags().StringVarP(&username, "username", "U", "", "basic auth username")
	cmd.PersistentFlags().StringVarP(&password, "password", "P", "", "basic auth password")
	cmd.PersistentFlags().StringVarP(&token, "token", "T", "", "JWT bearer token")

	return cmd
}

func prettyPrintPromises(cmd *cobra.Command, promises ...v1.Promise) {
	w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
	formatted := func(row ...any) {
		_, _ = fmt.Fprintf(w, "%v\t%v\t%v\t%v\n", row...)
	}

	formatted(
		"ID",
		"STATE",
		"TIMEOUT",
		"TAGS",
	)

	for _, promise := range promises {
		formatted(
			promise.Id,
			promise.State,
			promise.Timeout,
			strings.Join(util.PrettyHeaders(promise.Tags, ":"), " "),
		)
	}

	_ = w.Flush()
}

func prettyPrintPromise(cmd *cobra.Command, promise *v1.Promise) {
	w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)

	_, _ = fmt.Fprintf(w, "Id:\t%v\n", promise.Id)
	_, _ = fmt.Fprintf(w, "State:\t%s\n", promise.State)
	_, _ = fmt.Fprintf(w, "Timeout:\t%d\n", promise.Timeout)
	_, _ = fmt.Fprintf(w, "\n")

	_, _ = fmt.Fprintf(w, "Idempotency Key (create):\t%s\n", util.SafeDeref(promise.IdempotencyKeyForCreate))
	_, _ = fmt.Fprintf(w, "Idempotency Key (complete):\t%s\n", util.SafeDeref(promise.IdempotencyKeyForComplete))
	_, _ = fmt.Fprintf(w, "\n")

	_, _ = fmt.Fprintf(w, "Param:\n")
	_, _ = fmt.Fprintf(w, "\tHeaders:\n")
	for _, tag := range util.PrettyHeaders(util.SafeDeref(promise.Param.Headers), ":\t") {
		_, _ = fmt.Fprintf(w, "\t\t%s\n", tag)
	}
	_, _ = fmt.Fprintf(w, "\tData:\n")
	if promise.Param.Data != nil {
		_, _ = fmt.Fprintf(w, "\t\t%s\n", util.PrettyData(util.SafeDeref(promise.Param.Data)))
	}
	_, _ = fmt.Fprintf(w, "\n")

	_, _ = fmt.Fprintf(w, "Value:\n")
	_, _ = fmt.Fprintf(w, "\tHeaders:\n")
	for _, tag := range util.PrettyHeaders(util.SafeDeref(promise.Value.Headers), ":\t") {
		_, _ = fmt.Fprintf(w, "\t\t%s\n", tag)
	}
	_, _ = fmt.Fprintf(w, "\tData:\n")
	if promise.Value.Data != nil {
		_, _ = fmt.Fprintf(w, "\t\t%s\n", util.PrettyData(util.SafeDeref(promise.Value.Data)))
	}
	_, _ = fmt.Fprintf(w, "\n")

	_, _ = fmt.Fprintf(w, "Tags:\n")
	for _, tag := range util.PrettyHeaders(promise.Tags, ":\t") {
		_, _ = fmt.Fprintf(w, "\t%s\n", tag)
	}

	_ = w.Flush()
}

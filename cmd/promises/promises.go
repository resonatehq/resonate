package promises

import (
	"fmt"
	"strings"
	"text/tabwriter"

	"github.com/resonatehq/resonate/cmd/util"
	"github.com/resonatehq/resonate/pkg/client"
	"github.com/resonatehq/resonate/pkg/client/promises"
	"github.com/spf13/cobra"
)

func NewCmd(c client.ResonateClient) *cobra.Command {
	var (
		username string
		password string
	)

	cmd := &cobra.Command{
		Use:     "promises",
		Aliases: []string{"promise"},
		Short:   "Manage durable promises",
		Run: func(cmd *cobra.Command, args []string) {
			_ = cmd.Help()
		},
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			// Set basic auth if provided
			if username != "" || password != "" {
				c.SetBasicAuth(username, password)
			}
		},
	}

	// Add subcommands
	cmd.AddCommand(GetPromiseCmd(c))
	cmd.AddCommand(SearchPromisesCmd(c))
	cmd.AddCommand(CreatePromiseCmd(c))
	cmd.AddCommand(CompletePromiseCmds(c)...)

	// Flags
	cmd.PersistentFlags().StringVarP(&username, "username", "U", "", "Basic auth username")
	cmd.PersistentFlags().StringVarP(&password, "password", "P", "", "Basic auth password")

	return cmd
}

func prettyPrintPromises(cmd *cobra.Command, promises ...promises.Promise) {
	w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
	formatted := func(row ...any) {
		fmt.Fprintf(w, "%v\t%v\t%v\t%v\n", row...)
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

	w.Flush()
}

func prettyPrintPromise(cmd *cobra.Command, promise *promises.Promise) {
	w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)

	fmt.Fprintf(w, "Id:\t%v\n", promise.Id)
	fmt.Fprintf(w, "State:\t%s\n", promise.State)
	fmt.Fprintf(w, "Timeout:\t%d\n", promise.Timeout)
	fmt.Fprintf(w, "\n")

	fmt.Fprintf(w, "Idempotency Key (create):\t%s\n", util.SafeDeref(promise.IdempotencyKeyForCreate))
	fmt.Fprintf(w, "Idempotency Key (complete):\t%s\n", util.SafeDeref(promise.IdempotencyKeyForComplete))
	fmt.Fprintf(w, "\n")

	fmt.Fprintf(w, "Param:\n")
	fmt.Fprintf(w, "\tHeaders:\n")
	for _, tag := range util.PrettyHeaders(promise.Param.Headers, ":\t") {
		fmt.Fprintf(w, "\t\t%s\n", tag)
	}
	fmt.Fprintf(w, "\tData:\n")
	if promise.Param.Data != nil {
		fmt.Fprintf(w, "\t\t%s\n", util.PrettyData(promise.Param.Data))
	}
	fmt.Fprintf(w, "\n")

	fmt.Fprintf(w, "Value:\n")
	fmt.Fprintf(w, "\tHeaders:\n")
	for _, tag := range util.PrettyHeaders(promise.Value.Headers, ":\t") {
		fmt.Fprintf(w, "\t\t%s\n", tag)
	}
	fmt.Fprintf(w, "\tData:\n")
	if promise.Value.Data != nil {
		fmt.Fprintf(w, "\t\t%s\n", util.PrettyData(promise.Value.Data))
	}
	fmt.Fprintf(w, "\n")

	fmt.Fprintf(w, "Tags:\n")
	for _, tag := range util.PrettyHeaders(promise.Tags, ":\t") {
		fmt.Fprintf(w, "\t%s\n", tag)
	}

	w.Flush()
}

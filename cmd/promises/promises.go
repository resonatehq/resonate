package promises

import (
	"fmt"
	"strings"
	"text/tabwriter"

	"github.com/resonatehq/resonate/cmd/util"
	"github.com/resonatehq/resonate/pkg/client"
	"github.com/resonatehq/resonate/pkg/client/openapi"
	"github.com/spf13/cobra"
)

func NewCmd() *cobra.Command {
	var (
		c        = client.New()
		username string
		password string
	)

	cmd := &cobra.Command{
		Use:     "promises",
		Aliases: []string{"promise"},
		Short:   "Resonate promises",
		Run: func(cmd *cobra.Command, args []string) {
			_ = cmd.Help()
		},
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			server, err := cmd.Flags().GetString("server")
			if err != nil {
				return err
			}

			if username == "" && password == "" {
				return c.WithDefault(server)
			} else {
				return c.WithBasicAuth(server, username, password)
			}
		},
	}

	// Add subcommands
	cmd.AddCommand(GetPromiseCmd(c))
	cmd.AddCommand(SearchPromisesCmd(c))
	cmd.AddCommand(CreatePromiseCmd(c))
	cmd.AddCommand(CompletePromiseCmds(c)...)

	// Flags
	cmd.PersistentFlags().StringVarP(&username, "username", "U", "", "basic auth username")
	cmd.PersistentFlags().StringVarP(&password, "password", "P", "", "basic auth password")

	return cmd
}

func prettyPrintPromises(cmd *cobra.Command, promises ...openapi.Promise) {
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

func prettyPrintPromise(cmd *cobra.Command, promise *openapi.Promise) {
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
	for _, tag := range util.PrettyHeaders(util.SafeDeref(promise.Param.Headers), ":\t") {
		fmt.Fprintf(w, "\t\t%s\n", tag)
	}
	fmt.Fprintf(w, "\tData:\n")
	if promise.Param.Data != nil {
		fmt.Fprintf(w, "\t\t%s\n", util.PrettyData(util.SafeDeref(promise.Param.Data)))
	}
	fmt.Fprintf(w, "\n")

	fmt.Fprintf(w, "Value:\n")
	fmt.Fprintf(w, "\tHeaders:\n")
	for _, tag := range util.PrettyHeaders(util.SafeDeref(promise.Value.Headers), ":\t") {
		fmt.Fprintf(w, "\t\t%s\n", tag)
	}
	fmt.Fprintf(w, "\tData:\n")
	if promise.Value.Data != nil {
		fmt.Fprintf(w, "\t\t%s\n", util.PrettyData(util.SafeDeref(promise.Value.Data)))
	}
	fmt.Fprintf(w, "\n")

	fmt.Fprintf(w, "Tags:\n")
	for _, tag := range util.PrettyHeaders(promise.Tags, ":\t") {
		fmt.Fprintf(w, "\t%s\n", tag)
	}

	w.Flush()
}

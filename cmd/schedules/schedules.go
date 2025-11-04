package schedules

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
	)

	cmd := &cobra.Command{
		Use:     "schedules",
		Aliases: []string{"schedule"},
		Short:   "Resonate schedules",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if username != "" || password != "" {
				c.SetBasicAuth(username, password)
			}

			return c.Setup(server)
		},
		Run: func(cmd *cobra.Command, args []string) {
			_ = cmd.Help()
		},
	}

	// Add subcommands
	cmd.AddCommand(GetScheduleCmd(c))
	cmd.AddCommand(SearchSchedulesCmd(c))
	cmd.AddCommand(CreateScheduleCmd(c))
	cmd.AddCommand(DeleteScheduleCmd(c))

	// Flags
	cmd.PersistentFlags().StringVarP(&server, "server", "", "http://localhost:8001", "resonate url")
	cmd.PersistentFlags().StringVarP(&username, "username", "U", "", "basic auth username")
	cmd.PersistentFlags().StringVarP(&password, "password", "P", "", "basic auth password")

	return cmd
}

func prettyPrintSchedules(cmd *cobra.Command, schedules ...v1.Schedule) {
	w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
	formatted := func(row ...any) {
		_, _ = fmt.Fprintf(w, "%v\t%v\t%v\t%v\t%v\n", row...)
	}

	formatted(
		"ID",
		"CRON",
		"LAST RUN TIME",
		"NEXT RUN TIME",
		"TAGS",
	)

	for _, schedule := range schedules {
		formatted(
			schedule.Id,
			schedule.Cron,
			util.SafeDeref(schedule.LastRunTime),
			util.SafeDeref(schedule.NextRunTime),
			strings.Join(util.PrettyHeaders(schedule.Tags, ":"), " "),
		)
	}

	_ = w.Flush()
}

func prettyPrintSchedule(cmd *cobra.Command, schedule *v1.Schedule) {
	w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)

	_, _ = fmt.Fprintf(w, "Id:\t%v\n", schedule.Id)
	_, _ = fmt.Fprintf(w, "Description:\t%s\n", schedule.Description)
	_, _ = fmt.Fprintf(w, "\n")
	_, _ = fmt.Fprintf(w, "Cron:\t%s\n", schedule.Cron)
	_, _ = fmt.Fprintf(w, "Last run time:\t%d\n", util.SafeDeref(schedule.LastRunTime))
	_, _ = fmt.Fprintf(w, "Next run time:\t%d\n", util.SafeDeref(schedule.NextRunTime))
	_, _ = fmt.Fprintf(w, "\n")
	_, _ = fmt.Fprintf(w, "Tags:\n")
	for _, tag := range util.PrettyHeaders(schedule.Tags, ":\t") {
		_, _ = fmt.Fprintf(w, "\t%s\n", tag)
	}
	_, _ = fmt.Fprintf(w, "\n")

	_, _ = fmt.Fprintf(w, "Promise id:\t%s\n", schedule.PromiseId)
	_, _ = fmt.Fprintf(w, "Promise timeout:\t%d\n", schedule.PromiseTimeout)
	_, _ = fmt.Fprintf(w, "Promise param:\n")
	_, _ = fmt.Fprintf(w, "\tHeaders:\n")
	for _, tag := range util.PrettyHeaders(util.SafeDeref(schedule.PromiseParam.Headers), ":\t") {
		_, _ = fmt.Fprintf(w, "\t\t%s\n", tag)
	}
	_, _ = fmt.Fprintf(w, "\tData:\n")
	if schedule.PromiseParam.Data != nil {
		_, _ = fmt.Fprintf(w, "\t\t%s\n", util.PrettyData(util.SafeDeref(schedule.PromiseParam.Data)))
	}
	_, _ = fmt.Fprintf(w, "Promise tags:\n")
	for _, tag := range util.PrettyHeaders(schedule.PromiseTags, ":\t") {
		_, _ = fmt.Fprintf(w, "\t%s\n", tag)
	}

	_ = w.Flush()
}

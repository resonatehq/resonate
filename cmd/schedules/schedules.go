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
		Run: func(cmd *cobra.Command, args []string) {
			_ = cmd.Help()
		},
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			c.SetServer(server)

			if username != "" || password != "" {
				c.SetBasicAuth(username, password)
			}
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
		fmt.Fprintf(w, "%v\t%v\t%v\t%v\t%v\n", row...)
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

	w.Flush()
}

func prettyPrintSchedule(cmd *cobra.Command, schedule *v1.Schedule) {
	w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)

	fmt.Fprintf(w, "Id:\t%v\n", schedule.Id)
	fmt.Fprintf(w, "Description:\t%s\n", schedule.Description)
	fmt.Fprintf(w, "\n")
	fmt.Fprintf(w, "Cron:\t%s\n", schedule.Cron)
	fmt.Fprintf(w, "Last run time:\t%d\n", util.SafeDeref(schedule.LastRunTime))
	fmt.Fprintf(w, "Next run time:\t%d\n", util.SafeDeref(schedule.NextRunTime))
	fmt.Fprintf(w, "\n")
	fmt.Fprintf(w, "Tags:\n")
	for _, tag := range util.PrettyHeaders(schedule.Tags, ":\t") {
		fmt.Fprintf(w, "\t%s\n", tag)
	}
	fmt.Fprintf(w, "\n")

	fmt.Fprintf(w, "Promise id:\t%s\n", schedule.PromiseId)
	fmt.Fprintf(w, "Promise timeout:\t%d\n", schedule.PromiseTimeout)
	fmt.Fprintf(w, "Promise param:\n")
	fmt.Fprintf(w, "\tHeaders:\n")
	for _, tag := range util.PrettyHeaders(util.SafeDeref(schedule.PromiseParam.Headers), ":\t") {
		fmt.Fprintf(w, "\t\t%s\n", tag)
	}
	fmt.Fprintf(w, "\tData:\n")
	if schedule.PromiseParam.Data != nil {
		fmt.Fprintf(w, "\t\t%s\n", util.PrettyData(util.SafeDeref(schedule.PromiseParam.Data)))
	}
	fmt.Fprintf(w, "Promise tags:\n")
	for _, tag := range util.PrettyHeaders(schedule.PromiseTags, ":\t") {
		fmt.Fprintf(w, "\t%s\n", tag)
	}

	w.Flush()
}

package cmd

import "github.com/spf13/cobra"

// TODO: for now, get refactor in...
func init() {
	rootCmd.AddCommand(newScheduleCommand())
}

// http curl to the api server...
func newScheduleCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "schedule",
		Short: "schedule a recurring task",
		Run: func(cmd *cobra.Command, args []string) {

		},
	}

	cmd.AddCommand(newCreateScheduleCommand())
	cmd.AddCommand(newDeleteScheduleCommand())
	cmd.AddCommand(newDescribeScheduleCommand())

	return cmd
}

func newCreateScheduleCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "create",
		Short: "create a new schedule",
		Run: func(cmd *cobra.Command, args []string) {

		},
	}
}

func newDeleteScheduleCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "delete",
		Short: "delete a new schedule",
		Run: func(cmd *cobra.Command, args []string) {

		},
	}
}

func newDescribeScheduleCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "describe",
		Short: "describe a new schedule",
		Run: func(cmd *cobra.Command, args []string) {

		},
	}
}

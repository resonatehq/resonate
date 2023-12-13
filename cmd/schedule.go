package cmd

import (
	"context"
	"fmt"
	"io"

	"github.com/resonatehq/resonate/pkg/client"
	"github.com/spf13/cobra"
)

var apiServer string

func init() {
	apiServer = "http://0.0.0.0:8001"

	rootCmd.AddCommand(newScheduleCommand())
}

// todo: authentication and configuration of where the server is.

func newScheduleCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "schedule",
		Short: "Manage recurring schedules",
	}

	cmd.AddCommand(newCreateScheduleCommand())
	cmd.AddCommand(newDeleteScheduleCommand())
	cmd.AddCommand(newDescribeScheduleCommand())

	cmd.PersistentFlags().StringVar(&apiServer, "api", apiServer, "API server address")

	return cmd
}

func newCreateScheduleCommand() *cobra.Command {
	var desc, cron string

	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a new schedule",
		Run: func(cmd *cobra.Command, args []string) {
			// Get ID from args
			if len(args) != 1 {
				fmt.Println("Error: must specify ID")
				return
			}

			id := args[0]

			// Create API client
			c, err := client.NewClient(apiServer)
			if err != nil {
				panic(err)
			}

			// Build request
			body := client.CreateScheduleRequest{
				Id:   &id,
				Desc: &desc,
				Cron: &cron,
			}

			// Call API
			_, err = c.PostSchedules(context.TODO(), body)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}

			fmt.Println("Created schedule:", id)
		},
	}

	cmd.Flags().StringVarP(&desc, "desc", "d", "", "Description of schedule")
	cmd.Flags().StringVarP(&cron, "cron", "c", "", "CRON expression")

	return cmd
}

func newDeleteScheduleCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete [id]",
		Short: "Delete a schedule",
		Run: func(cmd *cobra.Command, args []string) {
			// Get ID from args
			if len(args) != 1 {
				fmt.Println("Error: must specify ID")
				return
			}

			id := args[0]

			c, err := client.NewClient(apiServer)
			if err != nil {
				panic(err)
			}

			_, err = c.DeleteSchedulesId(context.TODO(), id)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			fmt.Println("Deleted schedule", id)
		},
	}

	return cmd
}

func newDescribeScheduleCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "describe [id]",
		Short: "Get details of a schedule",
		Run: func(cmd *cobra.Command, args []string) {
			// Get ID from args
			if len(args) != 1 {
				fmt.Println("Error: must specify ID")
				return
			}

			id := args[0]

			c, err := client.NewClient(apiServer)
			if err != nil {
				panic(err)
			}

			// execute
			s, err := c.GetSchedulesId(context.TODO(), id)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}

			defer s.Body.Close()

			body, err := io.ReadAll(s.Body)
			if err != nil {
				panic(err)
			}

			fmt.Printf("%s", string(body))
		},
	}

	return cmd
}

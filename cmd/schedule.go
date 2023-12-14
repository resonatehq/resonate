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
	var id, promiseId, cron string
	var desc, promiseParam string

	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a new schedule",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 1 {
				fmt.Println("Error: must specify ID")
				return
			}
			id = args[0]

			c, err := client.NewClient(apiServer)
			if err != nil {
				panic(err)
			}

			body := client.Schedule{
				Id:           id,
				Desc:         &desc,
				Cron:         cron,
				PromiseId:    promiseId,
				PromiseParam: &promiseParam,
			}

			resp, err := c.PostSchedules(context.TODO(), body)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != 200 && resp.StatusCode != 201 {
				bs, err := io.ReadAll(resp.Body)
				if err != nil {
					panic(err)
				}
				fmt.Printf("%s\n", string(bs))
				return
			}

			fmt.Printf("Created schedule: %s\n", id)
		},
	}

	cmd.Flags().StringVarP(&desc, "desc", "d", "", "Description of schedule")
	cmd.Flags().StringVarP(&cron, "cron", "c", "", "CRON expression")
	cmd.Flags().StringVarP(&promiseId, "promise-id", "p", "", "ID of promise")
	cmd.Flags().StringVarP(&promiseParam, "promise-param", "a", "", "Parameter to pass to promise")

	_ = cmd.MarkFlagRequired("cron")
	_ = cmd.MarkFlagRequired("promise-id")

	return cmd
}

func newDeleteScheduleCommand() *cobra.Command {
	var id string

	cmd := &cobra.Command{
		Use:   "delete [id]",
		Short: "Delete a schedule",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 1 {
				fmt.Println("Error: must specify ID")
				return
			}
			id = args[0]

			c, err := client.NewClient(apiServer)
			if err != nil {
				panic(err)
			}

			resp, err := c.DeleteSchedulesId(context.TODO(), id)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != 200 {
				bs, err := io.ReadAll(resp.Body)
				if err != nil {
					panic(err)
				}
				fmt.Printf("%s\n", string(bs))
				return
			}

			fmt.Println("Deleted schedule:", id)
		},
	}

	return cmd
}

func newDescribeScheduleCommand() *cobra.Command {
	var id string

	cmd := &cobra.Command{
		Use:   "describe [id]",
		Short: "Get details of a schedule",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 1 {
				fmt.Println("Error: must specify ID")
				return
			}
			id = args[0]

			c, err := client.NewClient(apiServer)
			if err != nil {
				panic(err)
			}

			resp, err := c.GetSchedulesId(context.TODO(), id)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			defer resp.Body.Close()

			bs, err := io.ReadAll(resp.Body)
			if err != nil {
				panic(err)
			}

			if resp.StatusCode != 200 {
				fmt.Printf("%s\n", string(bs))
				return
			}

			fmt.Printf("%s\n", string(bs))
		},
	}

	return cmd
}

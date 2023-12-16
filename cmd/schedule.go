package cmd

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"

	"github.com/resonatehq/resonate/pkg/client"
	"github.com/resonatehq/resonate/pkg/client/schedules"
	"github.com/spf13/cobra"
)

func newScheduleCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "schedule",
		Short: "Manage schedules",
	}

	cmd.AddCommand(newCreateScheduleCommand())
	cmd.AddCommand(newDeleteScheduleCommand())
	cmd.AddCommand(newDescribeScheduleCommand())

	cmd.PersistentFlags().StringVar(&API, "api", API, "API server address")

	return cmd
}

func newCreateScheduleCommand() *cobra.Command {
	var (
		id, desc, cron, promiseId string
		promiseParamHeaders       map[string]string
		promiseParamData          string // base64 encoded data
		promiseTimeout            int64
	)

	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a new schedule",
		Example: `  
# Create a schedule that runs every 5 minutes.
resonate schedule create my-schedule --cron "*/5 * * * *" --promise-id "my-promise{{.timestamp}}" --promise-timeout 2524608000000

# Create a schedule that runs every 5 minutes and passes a data value to the promise.
resonate schedule create my-schedule --cron "*/5 * * * *" --promise-id "my-promise{{.timestamp}}" --promise-timeout 2524608000000 --data '{"foo": "bar"}' --headers Content-Type=application/json
`,
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 1 {
				fmt.Println("Error: must specify ID")
				return
			}
			id = args[0]

			c := client.NewOrDie(API)

			encodeBase64 := func(s string) *string {
				encoded := base64.StdEncoding.EncodeToString([]byte(s))
				return &encoded
			}

			body := schedules.Schedule{
				Id:        id,
				Desc:      &desc,
				Cron:      cron,
				PromiseId: promiseId,
				PromiseParam: &schedules.Value{
					Data:    encodeBase64(promiseParamData),
					Headers: &promiseParamHeaders,
				},
				PromiseTimeout: &promiseTimeout,
			}

			resp, err := c.SchedulesV1Alpha1().PostSchedules(context.TODO(), body)
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
	cmd.Flags().StringVarP(&promiseParamData, "data", "D", "", "Data value")
	cmd.Flags().StringToStringVarP(&promiseParamHeaders, "headers", "H", map[string]string{}, "Request headers")
	cmd.Flags().Int64VarP(&promiseTimeout, "promise-timeout", "t", 1, "Timeout for promise")

	_ = cmd.MarkFlagRequired("cron")
	_ = cmd.MarkFlagRequired("promise-id")
	_ = cmd.MarkFlagRequired("promise-timeout")

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

			c := client.NewOrDie(API)

			resp, err := c.SchedulesV1Alpha1().DeleteSchedulesId(context.TODO(), id)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != 204 {
				bs, err := io.ReadAll(resp.Body)
				if err != nil {
					panic(err)
				}
				fmt.Printf("Error: %s\n", string(bs))
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

			c := client.NewOrDie(API)

			resp, err := c.SchedulesV1Alpha1().GetSchedulesId(context.TODO(), id)
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

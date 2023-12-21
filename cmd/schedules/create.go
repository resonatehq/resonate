package schedules

import (
	"context"
	"encoding/base64"
	"time"

	"github.com/resonatehq/resonate/pkg/client"
	"github.com/resonatehq/resonate/pkg/client/schedules"
	"github.com/spf13/cobra"
)

var createScheduleExample = `
# Create a schedule that runs every minute
resonate schedule create foo --cron "* * * * *" --id "foo.{{.timestamp}}" --timeout 1h

# Create a schedule that runs every 5 minutes and includes data and headers
resonate schedule create foo --cron "*/5 * * * *" --id "foo.{{.timestamp}}" --timeout 1h --data foo --headers bar=bar`

func CreateScheduleCmd(c client.ResonateClient) *cobra.Command {
	var (
		id             string
		desc           string
		cron           string
		promiseId      string
		promiseTimeout time.Duration
		promiseData    string
		promiseHeaders map[string]string
	)

	cmd := &cobra.Command{
		Use:     "create <id>",
		Short:   "Create a durable schedule",
		Example: createScheduleExample,
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 1 {
				cmd.PrintErrln("Must specify schedule id")
				return
			}

			id = args[0]

			param := &schedules.ScheduleValue{}

			if cmd.Flag("header").Changed {
				param.Headers = &promiseHeaders
			}

			if cmd.Flag("data").Changed {
				encoded := base64.StdEncoding.EncodeToString([]byte(promiseData))
				param.Data = &encoded
			}

			body := schedules.Schedule{
				Id:             id,
				Cron:           cron,
				PromiseId:      promiseId,
				PromiseParam:   param,
				PromiseTimeout: promiseTimeout.Milliseconds(),
			}

			if cmd.Flag("desc").Changed {
				body.Desc = &desc
			}

			resp, err := c.SchedulesV1Alpha1().PostSchedulesWithResponse(context.TODO(), body)
			if err != nil {
				cmd.PrintErr(err)
				return
			}

			if resp.StatusCode() == 201 {
				cmd.Printf("Created schedule: %s\n", id)
			} else if resp.StatusCode() == 200 {
				cmd.Printf("Created schedule: %s (deduplicated)\n", id)
			} else {
				cmd.PrintErrln(string(resp.Body))
			}
		},
	}

	cmd.Flags().StringVarP(&desc, "desc", "d", "", "Schedule description")
	cmd.Flags().StringVarP(&cron, "cron", "c", "", "Schedule cron expression")
	cmd.Flags().StringVarP(&promiseId, "id", "i", "", "Templated schedule id, can include {{.timestamp}}")
	cmd.Flags().DurationVarP(&promiseTimeout, "timeout", "t", 0, "Promise timeout")
	cmd.Flags().StringVarP(&promiseData, "data", "D", "", "Promise param data")
	cmd.Flags().StringToStringVarP(&promiseHeaders, "header", "H", map[string]string{}, "Promise param header")

	_ = cmd.MarkFlagRequired("cron")
	_ = cmd.MarkFlagRequired("id")
	_ = cmd.MarkFlagRequired("timeout")

	return cmd
}

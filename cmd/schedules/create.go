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
resonate schedule create foo --cron "* * * * *" --promise-id "foo.{{.timestamp}}" --promise-timeout 1h

# Create a schedule that runs every 5 minutes and includes data and headers
resonate schedule create foo --cron "*/5 * * * *" --promise-id "foo.{{.timestamp}}" --promise-timeout 1h --promise-data foo --promise-header bar=bar`

func CreateScheduleCmd(c client.ResonateClient) *cobra.Command {
	var (
		id             string
		description    string
		cron           string
		tags           map[string]string
		promiseId      string
		promiseTimeout time.Duration
		promiseData    string
		promiseHeaders map[string]string
		promiseTags    map[string]string
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

			body := schedules.PostSchedulesJSONRequestBody{
				Id:             id,
				Cron:           cron,
				PromiseId:      promiseId,
				PromiseParam:   &schedules.PromiseValue{},
				PromiseTimeout: promiseTimeout.Milliseconds(),
			}

			if cmd.Flag("description").Changed {
				body.Description = &description
			}

			if cmd.Flag("tag").Changed {
				body.Tags = &tags
			}

			if cmd.Flag("promise-header").Changed {
				body.PromiseParam.Headers = promiseHeaders
			}

			if cmd.Flag("promise-data").Changed {
				encoded := base64.StdEncoding.EncodeToString([]byte(promiseData))
				body.PromiseParam.Data = &encoded
			}

			if cmd.Flag("promise-tag").Changed {
				body.PromiseTags = &promiseTags
			}

			resp, err := c.SchedulesV1Alpha1().PostSchedulesWithResponse(context.TODO(), nil, body)
			if err != nil {
				cmd.PrintErr(err)
				return
			}

			if resp.StatusCode() == 201 {
				cmd.Printf("Created schedule: %s\n", id)
			} else if resp.StatusCode() == 200 {
				cmd.Printf("Created schedule: %s (deduplicated)\n", id)
			} else {
				cmd.PrintErrln(resp.Status(), string(resp.Body))
			}
		},
	}

	cmd.Flags().StringVarP(&description, "description", "d", "", "Schedule description")
	cmd.Flags().StringVarP(&cron, "cron", "c", "", "Schedule cron expression")
	cmd.Flags().StringToStringVarP(&tags, "tag", "T", map[string]string{}, "Schedule tags")
	cmd.Flags().StringVar(&promiseId, "promise-id", "", "Templated schedule id, can include {{.timestamp}}")
	cmd.Flags().DurationVar(&promiseTimeout, "promise-timeout", 0, "Promise timeout")
	cmd.Flags().StringVar(&promiseData, "promise-data", "", "Promise param data")
	cmd.Flags().StringToStringVar(&promiseHeaders, "promise-header", map[string]string{}, "Promise param header")
	cmd.Flags().StringToStringVar(&promiseTags, "promise-tag", map[string]string{}, "Promise tags")

	_ = cmd.MarkFlagRequired("cron")
	_ = cmd.MarkFlagRequired("id")
	_ = cmd.MarkFlagRequired("timeout")

	return cmd
}

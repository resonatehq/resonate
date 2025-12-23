package schedules

import (
	"context"
	"encoding/base64"
	"errors"
	"time"

	"github.com/resonatehq/resonate/pkg/client"
	v1 "github.com/resonatehq/resonate/pkg/client/v1"
	"github.com/spf13/cobra"
)

var createScheduleExample = `
# Create a schedule that runs every minute
resonate schedules create foo --cron "* * * * *" --promise-timeout 1h --promise-id "foo.{{.timestamp}}"

# Create a schedule that runs every 5 minutes and includes data and headers
resonate schedules create foo --cron "*/5 * * * *" --promise-timeout 1h --promise-id "foo.{{.timestamp}}" --promise-data foo --promise-header bar=bar`

func CreateScheduleCmd(c client.Client) *cobra.Command {
	var (
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
		Short:   "Create schedule",
		Example: createScheduleExample,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return errors.New("must specify an id")
			}

			id := args[0]
			pt := promiseTimeout.Milliseconds()

			body := v1.CreateScheduleJSONRequestBody{
				Id:             &id,
				Cron:           &cron,
				PromiseId:      &promiseId,
				PromiseParam:   &v1.Value{},
				PromiseTimeout: &pt,
			}

			if cmd.Flag("description").Changed {
				body.Description = &description
			}

			if cmd.Flag("tag").Changed {
				body.Tags = &tags
			}

			if cmd.Flag("promise-header").Changed {
				body.PromiseParam.Headers = &promiseHeaders
			}

			if cmd.Flag("promise-data").Changed {
				encoded := base64.StdEncoding.EncodeToString([]byte(promiseData))
				body.PromiseParam.Data = &encoded
			}

			if cmd.Flag("promise-tag").Changed {
				body.PromiseTags = &promiseTags
			}

			res, err := c.V1().CreateScheduleWithResponse(context.TODO(), nil, body)
			if err != nil {
				return err
			}

			if res.StatusCode() == 201 {
				cmd.Printf("Created schedule: %s\n", id)
			} else if res.StatusCode() == 200 {
				cmd.Printf("Created schedule: %s (deduplicated)\n", id)
			} else {
				cmd.PrintErrln(res.Status(), string(res.Body))
			}

			return nil
		},
	}

	cmd.Flags().StringVarP(&cron, "cron", "c", "", "schedule cron expression")
	cmd.Flags().StringVarP(&promiseId, "promise-id", "i", "", "templated schedule id, can include {{.timestamp}}")
	cmd.Flags().DurationVarP(&promiseTimeout, "promise-timeout", "t", 0, "promise timeout")
	cmd.Flags().StringVar(&description, "description", "", "schedule description")
	cmd.Flags().StringToStringVar(&tags, "tag", map[string]string{}, "schedule tags")
	cmd.Flags().StringToStringVar(&promiseHeaders, "promise-header", map[string]string{}, "promise param header")
	cmd.Flags().StringVar(&promiseData, "promise-data", "", "promise param data")
	cmd.Flags().StringToStringVar(&promiseTags, "promise-tag", map[string]string{}, "promise tags")

	_ = cmd.MarkFlagRequired("cron")
	_ = cmd.MarkFlagRequired("promise-id")
	_ = cmd.MarkFlagRequired("promise-timeout")

	return cmd
}

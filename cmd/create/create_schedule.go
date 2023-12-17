package create

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"

	"github.com/resonatehq/resonate/pkg/client"
	"github.com/resonatehq/resonate/pkg/client/schedules"
	"github.com/spf13/cobra"
)

var createScheduleExample = `  
# Create a minimal schedule 
resonate create schedule my-schedule --cron "*/5 * * * *" --promise-id "my-promise{{.timestamp}}" --promise-timeout 2524608000000

# Create a schedule that runs every 5 minutes and passes a data value to the promise.
resonate create schedule my-schedule --cron "*/5 * * * *" --promise-id "my-promise{{.timestamp}}" --promise-timeout 2524608000000 --data '{"foo": "bar"}' --headers Content-Type=application/json
`

func NewCmdCreateSchedule(c client.ResonateClient) *cobra.Command {
	var (
		id, desc, cron, promiseId string
		promiseParamData          string
		promiseParamHeaders       map[string]string
		promiseTimeout            int64
	)

	cmd := &cobra.Command{
		Use:     "schedule",
		Short:   "Create a schedule resource",
		Example: createScheduleExample,
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 1 {
				fmt.Println("Error: must specify ID")
				return
			}
			id = args[0]

			encoded := base64.StdEncoding.EncodeToString([]byte(promiseParamData))

			body := schedules.Schedule{
				Id:        id,
				Desc:      &desc,
				Cron:      cron,
				PromiseId: promiseId,
				PromiseParam: &schedules.Value{
					Data:    &encoded,
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
	cmd.Flags().StringVarP(&promiseId, "promise-id", "p", "", "templated string for promise ID")
	cmd.Flags().StringVarP(&promiseParamData, "data", "D", "", "Promise data")
	cmd.Flags().StringToStringVarP(&promiseParamHeaders, "headers", "H", map[string]string{}, "Promise headers")
	cmd.Flags().Int64VarP(&promiseTimeout, "promise-timeout", "t", 1, "Timeout for promise")

	_ = cmd.MarkFlagRequired("cron")
	_ = cmd.MarkFlagRequired("promise-id")
	_ = cmd.MarkFlagRequired("promise-timeout")

	return cmd
}

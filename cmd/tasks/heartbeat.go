package tasks

import (
	"context"

	"github.com/resonatehq/resonate/pkg/client"
	v1 "github.com/resonatehq/resonate/pkg/client/v1"
	"github.com/spf13/cobra"
)

// Example command usage for sending a heartbeat to a task
var heartbeatTasksExample = `
# Heartbeat a task
resonate tasks heartbeat --pid bar`

// HeartbeatTaskCmd returns a cobra command for sending a heartbeat to a task.
func HeartbeatTaskCmd(c client.Client) *cobra.Command {
	var (
		pid string
	)

	// Define the cobra command
	cmd := &cobra.Command{
		Use:     "heartbeat",
		Short:   "Heartbeat tasks",
		Example: heartbeatTasksExample,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Create parameters for the request
			params := &v1.HeartbeatTasksParams{}

			// Create the body for heartbeat tasks request
			body := v1.HeartbeatTasksJSONRequestBody{
				ProcessId: pid,
			}

			// Call the client method to send the heartbeat (GET request with path params)
			res, err := c.V1().HeartbeatTasksWithResponse(context.TODO(), params, body)

			if err != nil {
				return err // Return any errors from the request
			}

			// Handle the response based on the status code
			if res.StatusCode() == 200 {
				cmd.Printf("Tasks heartbeated: %d\n", *res.JSON200.TasksAffected)
			} else {
				cmd.PrintErrln(res.Status(), string(res.Body))
				return nil
			}

			return nil // Return nil if no error occurred
		},
	}

	// Define command flags
	cmd.Flags().StringVarP(&pid, "pid", "p", "default", "claimant pid")

	return cmd
}

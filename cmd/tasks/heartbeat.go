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
resonate tasks heartbeat --process-id foo`

// HeartbeatTaskCmd returns a cobra command for sending a heartbeat to a task.
func HeartbeatTaskCmd(c client.Client) *cobra.Command {
	var (
		processId string // Unique process ID to heartbeat tasks
	)

	// Define the cobra command
	cmd := &cobra.Command{
		Use:     "heartbeat",
		Short:   "Send a heartbeat to a task",
		Example: heartbeatTasksExample,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Create parameters for the request
			params := &v1.HeartbeatTasksParams{}

			// Create the body for heartbeat tasks request
			body := v1.HeartbeatTasksJSONRequestBody{
				ProcessId: processId,
			}

			// Call the client method to send the heartbeat (GET request with path params)
			res, err := c.V1().HeartbeatTasksWithResponse(context.TODO(), params, body)

			if err != nil {
				return err // Return any errors from the request
			}

			// Handle the response based on the status code
			if res.StatusCode() == 200 {
				cmd.Printf("Heartbeat tasks for process-id: %s\n", processId)
			} else {
				cmd.PrintErrln(res.Status(), string(res.Body))
				return nil
			}

			return nil // Return nil if no error occurred
		},
	}

	// Define command flags
	cmd.Flags().StringVarP(&processId, "process-id", "p", "", "Unique process ID to heartbeat tasks")

	// Mark flags as required
	_ = cmd.MarkFlagRequired("process-id")

	return cmd // Return the constructed command
}

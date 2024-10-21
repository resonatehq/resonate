package tasks

import (
	"context"
	"errors"

	"github.com/resonatehq/resonate/pkg/client"
	v1 "github.com/resonatehq/resonate/pkg/client/v1"
	"github.com/spf13/cobra"
)

// Example command usage for sending a heartbeat to a task
var heartbeatTasksExample = `
# Heartbeat a task 
resonate tasks heartbeat foo --counter 1 --request-id bar`

// HeartbeatTaskCmd returns a cobra command for sending a heartbeat to a task.
func HeartbeatTaskCmd(c client.Client) *cobra.Command {
	var (
		counter   int    // Counter for the heartbeat
		requestId string // Unique tracking ID for the request
	)

	// Define the cobra command
	cmd := &cobra.Command{
		Use:     "heartbeat <id>",
		Short:   "Send a heartbeat to a task",
		Example: heartbeatTasksExample,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Validate required flags
			if len(args) != 1 {
				return errors.New("must specify an id")
			}

			id := args[0]

			if counter <= 0 {
				return errors.New("counter is required")
			}

			// Create parameters for the request
			params := &v1.HeartbeatTaskGetParams{
				RequestId: &requestId, // Set the request ID in the parameters
			}

			// Call the client method to send the heartbeat (GET request with path params)
			res, err := c.V1().HeartbeatTaskGetWithResponse(context.TODO(), id, counter, params)

			if err != nil {
				return err // Return any errors from the request
			}

			// Handle the response based on the status code
			if res.StatusCode() == 200 {
				cmd.Printf("Heartbeat sent for task: %s\n", id)
			} else {
				cmd.PrintErrln(res.Status(), string(res.Body))
				return nil
			}

			return nil // Return nil if no error occurred
		},
	}

	// Define command flags
	cmd.Flags().IntVarP(&counter, "counter", "c", 0, "The task counter")
	cmd.Flags().StringVarP(&requestId, "request-id", "r", "", "Unique tracking ID")

	// Mark flags as required
	_ = cmd.MarkFlagRequired("id")
	_ = cmd.MarkFlagRequired("counter")

	return cmd // Return the constructed command
}

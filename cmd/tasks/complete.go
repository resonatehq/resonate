package tasks

import (
	"context"
	"errors"

	"github.com/resonatehq/resonate/pkg/client"
	v1 "github.com/resonatehq/resonate/pkg/client/v1"
	"github.com/spf13/cobra"
)

// Example command usage for completing a task
var completeTasksExample = `
# Complete a task 
resonate tasks complete foo --counter 1`

// CompleteTaskCmd returns a cobra command for completing a task.
func CompleteTaskCmd(c client.Client) *cobra.Command {
	var (
		counter int // Counter for the task completion
	)

	// Define the cobra command
	cmd := &cobra.Command{
		Use:     "complete <id>",
		Short:   "Complete a task",
		Example: completeTasksExample,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Validate required flags
			if len(args) != 1 {
				return errors.New("must specify an id")
			}

			id := args[0]

			if counter <= 0 {
				return errors.New("counter is required")
			}

			// Create the body for the complete task request
			body := v1.CompleteTaskJSONRequestBody{
				Id:      id,
				Counter: counter,
			}

			// Call the client method to complete the task
			res, err := c.V1().CompleteTaskWithResponse(context.TODO(), body)

			if err != nil {
				return err // Return any errors from the request
			}

			// Handle the response based on the status code
			if res.StatusCode() == 201 {
				cmd.Printf("Task completed: %s\n", id)
			} else {
				cmd.PrintErrln(res.Status(), string(res.Body))
				return nil
			}

			return nil // Return nil if no error occurred
		},
	}

	// Define command flags
	cmd.Flags().IntVarP(&counter, "counter", "c", 0, "The task counter")

	// Mark flags as required
	_ = cmd.MarkFlagRequired("id")
	_ = cmd.MarkFlagRequired("counter")

	return cmd // Return the constructed command
}

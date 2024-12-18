package tasks

import (
	"context"
	"errors"
	"time"

	"github.com/resonatehq/resonate/pkg/client"
	v1 "github.com/resonatehq/resonate/pkg/client/v1"
	"github.com/spf13/cobra"
)

// Example command usage for claiming a task
var claimTasksExample = `
# Claim a task
resonate tasks claim foo --counter 1 --process-id bar --ttl 1m`

// ClaimTaskCmd returns a cobra command for claiming a task.
func ClaimTaskCmd(c client.Client) *cobra.Command {
	var (
		counter   int           // Counter for the task claim
		processId string        // Unique process ID identifying the claimer
		ttl       time.Duration // Time to live for the task claim
	)

	// Define the cobra command
	cmd := &cobra.Command{
		Use:     "claim <id>",
		Short:   "Claim a task",
		Example: claimTasksExample,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Validate required flags
			if len(args) != 1 {
				return errors.New("must specify an id")
			}

			id := args[0]

			if counter <= 0 {
				return errors.New("counter is required")
			}
			if processId == "" {
				return errors.New("process-id is required")
			}

			// Create parameters for the claim task request
			params := &v1.ClaimTaskParams{}

			// Create the body for the claim task request
			body := v1.ClaimTaskJSONRequestBody{
				Id:        id,
				Counter:   counter,
				ProcessId: processId,
				Ttl:       ttl.Milliseconds(), // Convert duration to milliseconds
			}

			// Call the client method to claim the task
			res, err := c.V1().ClaimTaskWithResponse(context.TODO(), params, body)

			if err != nil {
				return err // Return any errors from the request
			}

			// Handle the response based on the status code
			if res.StatusCode() == 201 {
				cmd.Printf("Task claimed: %s\n", id)
			} else {
				cmd.PrintErrln(res.Status(), string(res.Body))
				return nil
			}

			return nil // Return nil if no error occurred
		},
	}

	// Define command flags
	cmd.Flags().IntVarP(&counter, "counter", "c", 0, "task counter")
	cmd.Flags().StringVarP(&processId, "process-id", "p", "", "unique id that identifies the claimer")
	cmd.Flags().DurationVarP(&ttl, "ttl", "t", 0, "task time to live")

	// Mark flags as required
	_ = cmd.MarkFlagRequired("id")
	_ = cmd.MarkFlagRequired("counter")
	_ = cmd.MarkFlagRequired("process-id")
	_ = cmd.MarkFlagRequired("ttl")

	return cmd // Return the constructed command
}

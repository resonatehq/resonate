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
resonate tasks claim --id foo --counter 1 --process-id bar --ttl 1m`

// ClaimTaskCmd returns a cobra command for claiming a task.
func ClaimTaskCmd(c client.Client) *cobra.Command {
	var (
		id        string        // Task ID to claim
		counter   int           // Counter for the task claim
		processId string        // Unique process ID identifying the claimer
		ttl       time.Duration // Time to live for the task claim
		requestId string        // Unique tracking ID for the request
	)

	// Define the cobra command
	cmd := &cobra.Command{
		Use:     "claim",
		Short:   "Claim a task",
		Example: claimTasksExample,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Validate required flags
			if id == "" {
				return errors.New("id is required")
			}
			if counter <= 0 {
				return errors.New("counter is required")
			}
			if processId == "" {
				return errors.New("process-id is required")
			}

			// Create parameters for the claim task request
			params := &v1.ClaimTaskParams{
				RequestId: &requestId, // Optional tracking ID
			}

			// Create the body for the claim task request
			body := v1.ClaimTaskJSONRequestBody{
				Id:        id,
				Counter:   counter,
				ProcessId: processId,
				Ttl:       int64(ttl.Milliseconds()), // Convert duration to milliseconds
			}

			// Call the client method to claim the task
			res, err := c.V1().ClaimTaskWithResponse(context.TODO(), params, body)

			if err != nil {
				return err // Return any errors from the request
			}

			// Handle the response based on the status code
			if res.StatusCode() == 201 {
				cmd.Printf("Task claimed: %s\n", id)
			} else if res.StatusCode() == 403 {
				return errors.New("task already claimed, completed, or invalid counter")
			} else if res.StatusCode() == 404 {
				return errors.New("task not found")
			} else {
				cmd.PrintErrln(res.Status(), string(res.Body))
			}

			return nil // Return nil if no error occurred
		},
	}

	// Define command flags
	cmd.Flags().StringVarP(&id, "id", "i", "", "The task ID")
	cmd.Flags().IntVarP(&counter, "counter", "c", 0, "The task counter")
	cmd.Flags().StringVarP(&processId, "process-id", "p", "", "Unique process ID that identifies the claimer")
	cmd.Flags().DurationVarP(&ttl, "ttl", "t", 0, "Time to live in milliseconds")
	cmd.Flags().StringVarP(&requestId, "request-id", "r", "", "Unique tracking ID")

	// Mark flags as required
	_ = cmd.MarkFlagRequired("id")
	_ = cmd.MarkFlagRequired("counter")
	_ = cmd.MarkFlagRequired("process-id")
	_ = cmd.MarkFlagRequired("ttl")

	return cmd // Return the constructed command
}

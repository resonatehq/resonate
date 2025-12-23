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
resonate tasks claim foo --counter 1 --pid bar --ttl 1m`

// ClaimTaskCmd returns a cobra command for claiming a task.
func ClaimTaskCmd(c client.Client) *cobra.Command {
	var (
		counter int
		pid     string
		ttl     time.Duration
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
			if pid == "" {
				return errors.New("pid is required")
			}

			// Create parameters for the claim task request
			params := &v1.ClaimTaskParams{}

			// Create the body for the claim task request
			body := v1.ClaimTaskJSONRequestBody{
				Id:        id,
				Counter:   counter,
				ProcessId: pid,
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
	cmd.Flags().StringVar(&pid, "pid", "default", "claimant pid")
	cmd.Flags().DurationVar(&ttl, "ttl", time.Minute, "task ttl, time before which a heartbeat must be sent")

	// Mark flags as required
	_ = cmd.MarkFlagRequired("counter")

	return cmd
}

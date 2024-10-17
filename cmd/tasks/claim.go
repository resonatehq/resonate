package tasks

import (
	"time"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/app/coroutines"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/spf13/cobra"
)

var claimTaskExample = `
# Claim a task
resonate tasks claim --id foo --counter 1 --process-id bar --ttl 1m`

// ClaimTask sets up the CLI command for claiming a task using provided task information.
// It takes a gocoro.Coroutine as an argument which allows asynchronous task handling.
func ClaimTask(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any]) *cobra.Command {

	var (
		// Command-line flags for user input
		id        string
		counter   int
		processID string
		ttl       time.Duration
	)

	// Define the cobra command for `claim`
	cmd := &cobra.Command{
		Use:     "claim <id> <counter> <processID> <ttl>",
		Short:   "Claim Task",
		Example: claimTaskExample,
		Run: func(cmd *cobra.Command, args []string) {
			// Build the request for claiming a task with the user's input
			req := &t_api.Request{
				ClaimTask: &t_api.ClaimTaskRequest{
					Id:        id,
					Counter:   counter,
					ProcessId: processID,
					Ttl:       int(ttl.Seconds()),
				},
			}

			// Build the request for claiming a task
			resp, err := coroutines.ClaimTask(c, req)

			// Handle errors during task claiming
			if err != nil {
				cmd.PrintErrf("Error claiming task: %v\n", err)
				return
			}

			// If successful, display the claimed task information
			cmd.Printf("Claimed Task: %+v\n", resp.ClaimTask)
		},
	}

	// Define flags that allow users to pass values from the command line
	cmd.Flags().StringVar(&id, "id", "", "Task ID")
	cmd.Flags().IntVar(&counter, "counter", 1, "Task counter")
	cmd.Flags().StringVar(&processID, "process-id", "", "Process ID")
	cmd.Flags().DurationVar(&ttl, "ttl", 1*time.Minute, "Time to live (e.g: 1m, 30s)")

	// Mark certain flags as required so the user must provide them
	cmd.MarkFlagRequired("id")
	cmd.MarkFlagRequired("counter")
	cmd.MarkFlagRequired("process-id")
	cmd.MarkFlagRequired("ttl")

	return cmd
}

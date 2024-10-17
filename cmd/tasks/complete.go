package tasks

import (
	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/app/coroutines"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/spf13/cobra"
)

var completeTaskExample = `
# Complete a task
resonate tasks complete --id foo --counter 1`

// CompleteTask sets up the CLI command to complete a task.
func CompleteTask(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any]) *cobra.Command {
	var (
		id      string
		counter int
	)

	// Define the cobra command for `complete`
	cmd := &cobra.Command{
		Use:     "complete <id>",
		Short:   "Mark a task as complete",
		Example: completeTaskExample,
		Run: func(cmd *cobra.Command, args []string) {
			// Build the request for completing a task
			req := &t_api.Request{
				CompleteTask: &t_api.CompleteTaskRequest{
					Id:      id,
					Counter: counter,
				},
			}

			// Invoke the coroutine to complete the task
			resp, err := coroutines.CompleteTask(c, req)

			// Handle errors during task completion
			if err != nil {
				cmd.PrintErrf("Error completing task: %v\n", err)
				return
			}

			// Output the response, including the task completion status
			cmd.Printf("Task completion status: %s\n", resp.CompleteTask.Status)
			if resp.CompleteTask.Status == t_api.StatusCreated {
				cmd.Printf("Task '%s' marked as completed\n", id)
			}
		},
	}

	// Define flags for user input
	cmd.Flags().StringVar(&id, "id", "", "Task ID")
	cmd.Flags().IntVar(&counter, "counter", 1, "Counter")

	// Mark required flags
	cmd.MarkFlagRequired("id")
	cmd.MarkFlagRequired("counter")

	// Return the command to be added to the CLI app
	return cmd
}

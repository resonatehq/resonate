package t_conn

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/metadata"
)

const (
	HTTP ConnectionKind = "http"
	SQS  ConnectionKind = "aws.sqs"
)

type (
	ConnectionKind string

	ConnectionConfig struct {
		// kind identifies the type of queueing system to connect to (e.g. http, aws.sqs, etc.)
		Kind ConnectionKind

		// Name identifies the existing connection to the queueing system.
		Name string

		// Metadata is the any additional information or configuration for the connection.
		Metadata *metadata.Metadata
	}

	Links struct {
		Claim    string `json:"claim"`
		Complete string `json:"complete"`
	}

	ConnectionSubmission struct {
		// Queue is the task queue within the queueing system.
		Queue string `json:"queue"`

		// TaskId is the task id to submit.
		TaskId string `json:"taskid"`

		// Counter is the version of the task for claiming purposes.
		Counter int `json:"counter"`

		// Links are the links to claim and complete the specific task.
		Links Links `json:"links"`
	}

	Connection interface {
		// Init initializes the connection with the given tasks channel and metadata.
		Init(tasks <-chan *ConnectionSubmission, meta *metadata.Metadata) error

		// Tasks returns the tasks channel.
		Task() <-chan *ConnectionSubmission

		// Execute executes the given submission.
		Execute(sub *ConnectionSubmission) error

		// String returns the string representation of the connection.
		String() string
	}
)

// Start is  the main event loop for a connection.
// If the context is done, then the connection is stopped.
// If a task is received, then the connection executes the task.
// If the connection panics, then the connection recovers and logs the error.
func Start(ctx context.Context, c Connection) {
	for {
		if ctx.Err() != nil {
			return
		}

		eventLoop(ctx, c)
	}
}

func eventLoop(ctx context.Context, c Connection) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("http connection failed with error: %v", r)
			return
		}
	}()

	select {
	case <-ctx.Done():
		slog.Info(fmt.Sprintf("stopped connection %s with ctx.Done()", c.String()))
		return
	case task, ok := <-c.Task():
		if !ok {
			slog.Info(fmt.Sprintf("stopped connection %s with closed task queue", c.String()))
			return
		}

		slog.Info(fmt.Sprintf("connection %s received task %s", c.String(), task.TaskId))
		err := c.Execute(task)
		if err != nil {
			slog.Error(fmt.Errorf("connection %s failed to complete task with error: %q", c.String(), err).Error())
		}

		slog.Info(fmt.Sprintf("connection %s submitted task %s", c.String(), task.TaskId))
	}
}

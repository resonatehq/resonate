package t_bind

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/metadata"
)

type BindingKind string

const (
	HTTP BindingKind = "http"
	SQS  BindingKind = "aws.sqs"
)

type Config struct {
	Kind     BindingKind
	Name     string
	Metadata *metadata.Metadata
}

type BindingSubmission struct {
	TaskId  string `json:"taskid"`
	Counter int    `json:"counter"`
}

type Binding interface {
	Init(tasks <-chan *BindingSubmission, meta *metadata.Metadata) error
	Task() <-chan *BindingSubmission
	Execute(sub *BindingSubmission) error
	String() string
}

func Start(ctx context.Context, b Binding) {
	defer func() { // proopagata ?
		if r := recover(); r != nil {
			slog.Error("http connection failed with error: %v", r)
			// send response back. retry logic.
		}
	}()

	// todo: retry logic if queuing fails.

	for {
		select {
		case <-ctx.Done():
			slog.Info(fmt.Sprintf("stopped binding %s with ctx.Done()", b.String()))
			return
		case task, ok := <-b.Task():
			if !ok {
				slog.Info(fmt.Sprintf("stopped binding %s with closed task queue", b.String()))
				return
			}

			err := b.Execute(task)
			if err != nil {
				slog.Error(fmt.Errorf("binding %s failed to complete task with error: %q", b.String(), err).Error())
			}
		}
	}
}

package bus

import (
	"fmt"

	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
)

type Input interface {
	t_aio.Submission | t_api.Request
}

type Output interface {
	t_aio.Completion | t_api.Response
}

type SQE[I Input, O Output] struct {
	// Metadata   *metadata.Metadata
	Submission *I
	Callback   func(*O, error)
}

func (sqe *SQE[I, O]) String() string {
	return fmt.Sprintf("SQE(submission=%v)", sqe.Submission)
}

type CQE[I Input, O Output] struct {
	Completion *O
	Callback   func(*O, error)
	Error      error
}

func (cqe *CQE[I, O]) Invoke() {
	cqe.Callback(cqe.Completion, cqe.Error)
}

func (cqe *CQE[I, O]) String() string {
	return fmt.Sprintf("CQE(completion=%v, error=%v)", cqe.Completion, cqe.Error)
}

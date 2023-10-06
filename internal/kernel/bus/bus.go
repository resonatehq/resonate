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
	Tags       string
	Submission *I
	Callback   func(*O, error)
}

func (sqe *SQE[I, O]) String() string {
	return fmt.Sprintf("SQE(tags=%s, submission=%v)", sqe.Tags, sqe.Submission)
}

type CQE[I Input, O Output] struct {
	Tags       string
	Completion *O
	Callback   func(*O, error)
	Error      error
}

func (cqe *CQE[I, O]) String() string {
	return fmt.Sprintf("CQE(tags=%s, completion=%v, error=%v)", cqe.Tags, cqe.Completion, cqe.Error)
}

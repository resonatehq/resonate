package bus

import (
	"fmt"

	"github.com/resonatehq/resonate/internal/kernel/metadata"
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
	Metadata   *metadata.Metadata
	Submission *I
	Callback   func(*O, *t_api.PlatformLevelError)
}

func (sqe *SQE[I, O]) String() string {
	return fmt.Sprintf("SQE(metadata=%s, submission=%v)", sqe.Metadata, sqe.Submission)
}

type CQE[I Input, O Output] struct {
	Metadata   *metadata.Metadata
	Completion *O
	Callback   func(*O, *t_api.PlatformLevelError)
	Error      *t_api.PlatformLevelError
}

func (cqe *CQE[I, O]) String() string {
	return fmt.Sprintf("CQE(metadata=%s, completion=%v, error=%v)", cqe.Metadata, cqe.Completion, cqe.Error)
}

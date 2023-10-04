package bus

import (
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
	Callback   func(int64, *O, error)
}

type CQE[I Input, O Output] struct {
	Tags       string
	Completion *O
	Callback   func(int64, *O, error)
	Error      error
}

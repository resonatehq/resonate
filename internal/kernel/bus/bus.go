package bus

import "github.com/resonatehq/resonate/internal/kernel/types"

type Submission interface {
	types.Submission | types.Request
}

type Completion interface {
	types.Completion | types.Response
}

type SQE[S, C any] struct {
	Submission *S
	Callback   func(*C, error)
}

type CQE[S, C any] struct {
	Completion *C
	Callback   func(*C, error)
	Error      error
}

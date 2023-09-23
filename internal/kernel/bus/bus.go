package bus

import (
	"github.com/resonatehq/resonate/internal/kernel/types"
)

type Submission interface {
	types.Submission | types.Request
}

type Completion interface {
	types.Completion | types.Response
}

type SQE[S Submission, C Completion] struct {
	Kind       string
	Submission *S
	Callback   func(int64, *C, error)
}

type CQE[S Submission, C Completion] struct {
	Kind       string
	Completion *C
	Callback   func(int64, *C, error)
	Error      error
}

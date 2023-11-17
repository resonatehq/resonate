package util

import (
	"fmt"

	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/pkg/promise"
)

func GetForbiddenStatus(currentState promise.State) t_api.ResponseStatus {
	switch currentState {
	case promise.Resolved:
		return t_api.StatusPromiseAlreadyResolved
	case promise.Rejected:
		return t_api.StatusPromiseAlreadyRejected
	case promise.Canceled:
		return t_api.StatusPromiseAlreadyCanceled
	case promise.Timedout:
		return t_api.StatusPromiseAlreadyTimedOut
	default:
		panic(fmt.Sprintf("invalid promise state: %v", currentState))
	}
}

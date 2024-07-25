package network

import (
	"math/rand" // nosemgrep
	"net/http"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/util"
)

type ConfigDST struct {
	P float32
}

type NetworkDST struct {
	config *ConfigDST
	r      *rand.Rand
}

type NetworkDSTDevice struct {
	*NetworkDST
}

func NewDST(config *ConfigDST, r *rand.Rand) aio.Subsystem {
	return &NetworkDST{
		config: config,
		r:      r,
	}
}

func (n *NetworkDST) String() string {
	return "network:dst"
}

func (n *NetworkDST) Start() error {
	return nil
}

func (n *NetworkDST) Stop() error {
	return nil
}

func (n *NetworkDST) Reset() error {
	return nil
}

func (n *NetworkDST) NewWorker(int) aio.Worker {
	return &NetworkDSTDevice{n}
}

func (d *NetworkDSTDevice) Process(sqes []*bus.SQE[t_aio.Submission, t_aio.Completion]) []*bus.CQE[t_aio.Submission, t_aio.Completion] {
	cqes := make([]*bus.CQE[t_aio.Submission, t_aio.Completion], len(sqes))

	for i, sqe := range sqes {
		util.Assert(sqe.Submission.Network != nil, "submission must not be nil")

		switch sqe.Submission.Network.Kind {
		case t_aio.Http:
			cqe := &bus.CQE[t_aio.Submission, t_aio.Completion]{
				Callback: sqe.Callback,
			}

			var res *http.Response

			if d.r.Float32() < d.config.P {
				res = &http.Response{
					StatusCode: http.StatusOK,
				}
			} else {
				res = &http.Response{
					StatusCode: http.StatusInternalServerError,
				}
			}

			cqe.Completion = &t_aio.Completion{
				Kind: t_aio.Network,
				Tags: sqe.Submission.Tags, // propagate the tags
				Network: &t_aio.NetworkCompletion{
					Kind: t_aio.Http,
					Http: res,
				},
			}

			cqes[i] = cqe
		default:
			panic("invalid network submission")
		}
	}

	return cqes
}

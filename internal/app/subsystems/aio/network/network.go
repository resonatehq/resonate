package network

import (
	"bytes"
	"net/http"
	"time"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/resonatehq/resonate/internal/util"
)

type Network struct {
	timeout time.Duration
}

type NetworkDevice struct {
	client *http.Client
}

func New(timeout time.Duration) aio.Subsystem {
	return &Network{
		timeout: timeout,
	}
}

func (n *Network) String() string {
	return "network"
}

func (n *Network) Start() error {
	return nil
}

func (n *Network) Stop() error {
	return nil
}

func (n *Network) Reset() error {
	return nil
}

func (n *Network) NewWorker(int) aio.Worker {
	return &NetworkDevice{
		client: &http.Client{
			Timeout: n.timeout,
		},
	}
}

func (d *NetworkDevice) Process(sqes []*bus.SQE[types.Submission, types.Completion]) []*bus.CQE[types.Submission, types.Completion] {
	cqes := make([]*bus.CQE[types.Submission, types.Completion], len(sqes))

	for i, sqe := range sqes {
		util.Assert(sqe.Submission.Network != nil, "submission must not be nil")

		switch sqe.Submission.Network.Kind {
		case types.Http:
			cqe := &bus.CQE[types.Submission, types.Completion]{
				Callback: sqe.Callback,
			}

			res, err := d.httpRequest(sqe.Submission.Network.Http)
			if err != nil {
				cqe.Error = err
			} else {
				cqe.Completion = &types.Completion{
					Kind: types.Network,
					Network: &types.NetworkCompletion{
						Kind: types.Http,
						Http: res,
					},
				}
			}

			cqes[i] = cqe
		default:
			panic("invalid network submission")
		}
	}

	return cqes
}

func (d *NetworkDevice) httpRequest(r *types.HttpRequest) (*http.Response, error) {
	req, err := http.NewRequest(r.Method, r.Url, bytes.NewBuffer(r.Body))
	if err != nil {
		return nil, err
	}

	for key, value := range r.Headers {
		req.Header.Set(key, value)
	}

	return d.client.Do(req)
}

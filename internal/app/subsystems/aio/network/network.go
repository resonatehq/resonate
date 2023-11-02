package network

import (
	"bytes"
	"net/http"
	"time"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/util"
)

type Config struct {
	Timeout time.Duration
}

type Network struct {
	config *Config
}

type NetworkDevice struct {
	client *http.Client
}

func New(config *Config) aio.Subsystem {
	return &Network{
		config: config,
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
			Timeout: n.config.Timeout,
		},
	}
}

func (d *NetworkDevice) Process(sqes []*bus.SQE[t_aio.Submission, t_aio.Completion]) []*bus.CQE[t_aio.Submission, t_aio.Completion] {
	cqes := make([]*bus.CQE[t_aio.Submission, t_aio.Completion], len(sqes))

	for i, sqe := range sqes {
		util.Assert(sqe.Submission.Network != nil, "submission must not be nil")

		switch sqe.Submission.Network.Kind {
		case t_aio.Http:
			cqe := &bus.CQE[t_aio.Submission, t_aio.Completion]{
				Metadata: sqe.Metadata,
				Callback: sqe.Callback,
			}

			res, err := d.httpRequest(sqe.Submission.Network.Http)
			if err != nil {
				cqe.Error = err
			} else {
				cqe.Completion = &t_aio.Completion{
					Kind: t_aio.Network,
					Network: &t_aio.NetworkCompletion{
						Kind: t_aio.Http,
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

func (d *NetworkDevice) httpRequest(r *t_aio.HttpRequest) (*http.Response, error) {
	req, err := http.NewRequest(r.Method, r.Url, bytes.NewBuffer(r.Body))
	if err != nil {
		return nil, err
	}

	for key, value := range r.Headers {
		req.Header.Set(key, value)
	}

	return d.client.Do(req)
}

package sender

import (
	"fmt"
	"math/rand" // nosemgrep

	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
)

// Config

type ConfigDST struct {
	P float64 `flag:"p" desc:"probability of simulated unsuccessful request" default:"0.5" dst:"0:1"`
}

// Subsystem

type SenderDST struct {
	config      *ConfigDST
	r           *rand.Rand
	backchannel chan interface{}
}

func NewDST(r *rand.Rand, backchannel chan interface{}, config *ConfigDST) (*SenderDST, error) {
	return &SenderDST{
		config:      config,
		r:           r,
		backchannel: backchannel,
	}, nil
}

func (s *SenderDST) String() string {
	return fmt.Sprintf("%s:dst", t_aio.Sender.String())
}

func (s *SenderDST) Kind() t_aio.Kind {
	return t_aio.Sender
}

func (s *SenderDST) Start() error {
	return nil
}

func (s *SenderDST) Stop() error {
	return nil
}

func (s *SenderDST) Process(sqes []*bus.SQE[t_aio.Submission, t_aio.Completion]) []*bus.CQE[t_aio.Submission, t_aio.Completion] {
	cqes := make([]*bus.CQE[t_aio.Submission, t_aio.Completion], len(sqes))

	for i, sqe := range sqes {
		var completion *t_aio.SenderCompletion

		select {
		case s.backchannel <- sqe.Submission.Sender.Task:
			completion = &t_aio.SenderCompletion{
				Success: s.r.Float64() < s.config.P,
			}
		default:
			completion = &t_aio.SenderCompletion{
				Success: false,
			}
		}

		cqes[i] = &bus.CQE[t_aio.Submission, t_aio.Completion]{
			Id: sqe.Id,
			Completion: &t_aio.Completion{
				Kind:   t_aio.Sender,
				Tags:   sqe.Submission.Tags, // propagate the tags
				Sender: completion,
			},
			Callback: sqe.Callback,
		}
	}

	return cqes
}

package sender

import (
	"fmt"
	"math/rand" // nosemgrep

	"github.com/go-viper/mapstructure/v2"
	cmdUtil "github.com/resonatehq/resonate/cmd/util"
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/resonatehq/resonate/pkg/message"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Config

type ConfigDST struct {
	P float64 `flag:"p" desc:"probability of simulated unsuccessful request" default:"0.5" run:"0:1"`
}

func (c *ConfigDST) Bind(cmd *cobra.Command, vip *viper.Viper, prefix string, keyPrefix string) {
	cmdUtil.Bind(c, cmd, vip, prefix, keyPrefix)
}

func (c *ConfigDST) BindPersistent(cmd *cobra.Command, vip *viper.Viper, prefix string, keyPrefix string) {
	cmdUtil.BindPersistent(c, cmd, vip, prefix, keyPrefix)
}

func (c *ConfigDST) Decode(value any, decodeHook mapstructure.DecodeHookFunc) error {
	decoderConfig := &mapstructure.DecoderConfig{
		Result:     c,
		DecodeHook: decodeHook,
	}

	decoder, err := mapstructure.NewDecoder(decoderConfig)
	if err != nil {
		return err
	}

	if err := decoder.Decode(value); err != nil {
		return err
	}

	return nil
}

func (c *ConfigDST) New(aio.AIO, *metrics.Metrics) (aio.Subsystem, error) {
	panic("not implemented")
}

func (c *ConfigDST) NewDST(aio aio.AIO, metrics *metrics.Metrics, r *rand.Rand, backchannel chan interface{}) (aio.SubsystemDST, error) {
	return NewDST(r, backchannel, c)
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

func (s *SenderDST) Start(chan<- error) error {
	return nil
}

func (s *SenderDST) Stop() error {
	return nil
}

func (s *SenderDST) Process(sqes []*bus.SQE[t_aio.Submission, t_aio.Completion]) []*bus.CQE[t_aio.Submission, t_aio.Completion] {
	cqes := make([]*bus.CQE[t_aio.Submission, t_aio.Completion], len(sqes))

	for i, sqe := range sqes {
		var completion *t_aio.SenderCompletion

		mesgType := sqe.Submission.Sender.Task.Mesg.Type

		var obj any
		if mesgType == message.Notify {
			obj = sqe.Submission.Sender.Promise
		} else {
			obj = sqe.Submission.Sender.Task
		}

		select {
		case s.backchannel <- obj:
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

package nats

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/metrics"
)

type MockNATSClient struct {
	ch chan<- *PublishParams
	ok bool
}

type PublishParams struct {
	Subject string
	Data    []byte
}

func (m *MockNATSClient) Publish(subject string, data []byte) error {
	if !m.ok {
		return errors.New("mock error: failed to publish message")
	}

	// send the params to the backchannel
	m.ch <- &PublishParams{
		Subject: subject,
		Data:    data,
	}

	return nil
}

func TestNATSPlugin(t *testing.T) {
	metrics := metrics.New(prometheus.NewRegistry())

	// create a backchannel
	ch := make(chan *PublishParams, 1)
	defer close(ch)

	successClient := &MockNATSClient{ch, true}
	failureClient := &MockNATSClient{ch, false}

	for _, tc := range []struct {
		name    string
		addr    []byte
		client  *MockNATSClient
		success bool
		params  *PublishParams
	}{
		{
			name:    "Success",
			addr:    []byte(`{"subject": "tasks.process"}`),
			client:  successClient,
			success: true,
			params: &PublishParams{
				Subject: "tasks.process",
				Data:    []byte("test message"),
			},
		},
		{
			name:    "SuccessWithWildcard",
			addr:    []byte(`{"subject": "tasks.*.process"}`),
			client:  successClient,
			success: true,
			params: &PublishParams{
				Subject: "tasks.*.process",
				Data:    []byte("test message"),
			},
		},
		{
			name:    "FailureDueToJson",
			addr:    []byte(""),
			client:  successClient,
			success: false,
		},
		{
			name:    "FailureDueToEmptySubject",
			addr:    []byte(`{}`),
			client:  successClient,
			success: false,
		},
		{
			name:    "FailureDueToClient",
			addr:    []byte(`{"subject": "tasks.process"}`),
			client:  failureClient,
			success: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			natsPlugin, err := newWithClient(nil, metrics, &Config{Size: 1, Workers: 1}, successClient)
			assert.Nil(t, err)

			err = natsPlugin.Start(nil)
			assert.Nil(t, err)

			ok := natsPlugin.Enqueue(&aio.Message{
				Addr: tc.addr,
				Body: []byte("test message"),
				Done: func(completion *t_aio.SenderCompletion) {
					assert.Equal(t, tc.success, completion.Success)
				},
			})

			assert.True(t, ok)

			if tc.success {
				assert.Equal(t, tc.params, <-ch)
			}

			err = natsPlugin.Stop()
			assert.Nil(t, err)
		})
	}
}

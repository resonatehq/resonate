package kafka

import (
	"errors"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/metrics"
)

type MockKafkaProducer struct {
	ch chan<- *SendMessageParams
	ok bool
}

type SendMessageParams struct {
	Topic   string
	Key     string
	Value   []byte
	Headers map[string]string
}

func (m *MockKafkaProducer) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	if !m.ok {
		return 0, 0, errors.New("mock error")
	}

	headers := make(map[string]string)
	for _, h := range msg.Headers {
		headers[string(h.Key)] = string(h.Value)
	}

	key := ""
	if msg.Key != nil {
		if se, ok := msg.Key.(sarama.StringEncoder); ok {
			key = string(se)
		}
	}

	m.ch <- &SendMessageParams{
		Topic:   msg.Topic,
		Key:     key,
		Value:   msg.Value.(sarama.ByteEncoder),
		Headers: headers,
	}

	return 0, 12345, nil
}

func (m *MockKafkaProducer) Close() error {
	return nil
}

func TestKafkaPlugin(t *testing.T) {
	metrics := metrics.New(prometheus.NewRegistry())

	ch := make(chan *SendMessageParams, 1)
	defer close(ch)

	successProducer := &MockKafkaProducer{ch, true}
	failureProducer := &MockKafkaProducer{ch, false}

	for _, tc := range []struct {
		name     string
		addr     []byte
		producer *MockKafkaProducer
		success  bool
		params   *SendMessageParams
	}{
		{
			name:     "Success",
			addr:     []byte(`{"topic": "test-topic"}`),
			producer: successProducer,
			success:  true,
			params: &SendMessageParams{
				Topic:   "test-topic",
				Key:     "",
				Value:   []byte("test message"),
				Headers: map[string]string{},
			},
		},
		{
			name:     "SuccessWithKey",
			addr:     []byte(`{"topic": "test-topic", "key": "test-key"}`),
			producer: successProducer,
			success:  true,
			params: &SendMessageParams{
				Topic:   "test-topic",
				Key:     "test-key",
				Value:   []byte("test message"),
				Headers: map[string]string{},
			},
		},
		{
			name:     "SuccessWithHeaders",
			addr:     []byte(`{"topic": "test-topic", "headers": {"content-type": "application/json", "source": "resonate"}}`),
			producer: successProducer,
			success:  true,
			params: &SendMessageParams{
				Topic:   "test-topic",
				Key:     "",
				Value:   []byte("test message"),
				Headers: map[string]string{"content-type": "application/json", "source": "resonate"},
			},
		},
		{
			name:     "FailureDueToJson",
			addr:     []byte(""),
			producer: successProducer,
			success:  false,
		},
		{
			name:     "FailureDueToMissingTopic",
			addr:     []byte(`{}`),
			producer: successProducer,
			success:  false,
		},
		{
			name:     "FailureDueToProducer",
			addr:     []byte(`{"topic": "test-topic"}`),
			producer: failureProducer,
			success:  false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kafka, err := NewWithProducer(nil, metrics, &Config{
				Size:    1,
				Workers: 1,
				Timeout: 100 * time.Millisecond,
			}, tc.producer)
			assert.Nil(t, err)

			err = kafka.Start(nil)
			assert.Nil(t, err)

			ok := kafka.Enqueue(&aio.Message{
				Addr: tc.addr,
				Body: []byte("test message"),
				Done: func(completion *t_aio.SenderCompletion) {
					assert.Equal(t, tc.success, completion.Success)
				},
			})

			assert.True(t, ok)

			if tc.success {
				select {
				case params := <-ch:
					assert.Equal(t, tc.params, params)
				case <-time.After(200 * time.Millisecond):
					t.Fatal("timeout waiting for message")
				}
			}

			err = kafka.Stop()
			assert.Nil(t, err)
		})
	}
}

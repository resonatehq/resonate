package kafka

import (
	"errors"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
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

func (m *MockKafkaProducer) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	if !m.ok {
		return errors.New("mock error")
	}

	headers := make(map[string]string)
	for _, h := range msg.Headers {
		headers[h.Key] = string(h.Value)
	}

	key := ""
	if msg.Key != nil {
		key = string(msg.Key)
	}

	topic := ""
	if msg.TopicPartition.Topic != nil {
		topic = *msg.TopicPartition.Topic
	}

	if deliveryChan != nil {
		deliveryChan <- &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
		}
	}

	m.ch <- &SendMessageParams{
		Topic:   topic,
		Key:     key,
		Value:   msg.Value,
		Headers: headers,
	}

	return nil
}

func (m *MockKafkaProducer) Close() {}

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
			name:     "SuccessWithHeadParameter",
			addr:     []byte(`{"topic": "test-topic"}`),
			producer: successProducer,
			success:  true,
			params: &SendMessageParams{
				Topic:   "test-topic",
				Key:     "",
				Value:   []byte("test message"),
				Headers: map[string]string{"foo": "bar", "baz": "qux"},
			},
		},
		{
			name:     "SuccessWithHeadAndAddrHeaders",
			addr:     []byte(`{"topic": "test-topic", "headers": {"content-type": "application/json", "source": "resonate"}}`),
			producer: successProducer,
			success:  true,
			params: &SendMessageParams{
				Topic:   "test-topic",
				Key:     "",
				Value:   []byte("test message"),
				Headers: map[string]string{"content-type": "application/json", "source": "resonate", "foo": "bar", "baz": "qux"},
			},
		},
		{
			name:     "SuccessWithHeadPrecedenceOverAddrHeaders",
			addr:     []byte(`{"topic": "test-topic", "headers": {"source": "addr", "conflict": "addr-value"}}`),
			producer: successProducer,
			success:  true,
			params: &SendMessageParams{
				Topic:   "test-topic",
				Key:     "",
				Value:   []byte("test message"),
				Headers: map[string]string{"source": "head", "conflict": "head-value", "foo": "bar", "baz": "qux"},
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

			msg := &aio.Message{
				Addr: tc.addr,
				Body: []byte("test message"),
				Done: func(completion *t_aio.SenderCompletion) {
					assert.Equal(t, tc.success, completion.Success)
				},
			}

			// Add Head parameter for tests that need it
			if tc.name == "SuccessWithHeadParameter" || tc.name == "SuccessWithHeadAndAddrHeaders" || tc.name == "SuccessWithHeadPrecedenceOverAddrHeaders" {
				msg.Head = map[string]string{
					"foo": "bar",
					"baz": "qux",
				}
			}
			if tc.name == "SuccessWithHeadPrecedenceOverAddrHeaders" {
				msg.Head["source"] = "head"
				msg.Head["conflict"] = "head-value"
			}

			ok := kafka.Enqueue(msg)

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

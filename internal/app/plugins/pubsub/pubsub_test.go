package pubsub

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/stretchr/testify/assert"
)

type MockPubSubClient struct {
	ch chan<- *request
	ok bool
}

type request struct {
	topic string
	body  []byte
}

func (m *MockPubSubClient) Publish(ctx context.Context, topic string, data []byte) (string, error) {
	if !m.ok {
		return "", errors.New("mock error: failed to publish message")
	}
	m.ch <- &request{topic: topic, body: data}
	return "test-message-id", nil
}

func (m *MockPubSubClient) Close() error {
	return nil
}

func TestPubSubPlugin(t *testing.T) {
	metrics := metrics.New(prometheus.NewRegistry())

	ch := make(chan *request, 1)
	defer close(ch)

	successClient := &MockPubSubClient{ch, true}
	failureClient := &MockPubSubClient{ch, false}

	for _, tc := range []struct {
		name    string
		addr    []byte
		client  *MockPubSubClient
		success bool
		request *request
	}{
		{
			name:    "Success",
			addr:    []byte(`{"topic": "test-topic"}`),
			client:  successClient,
			success: true,
			request: &request{
				topic: "test-topic",
				body:  []byte("test message"),
			},
		},
		{
			name:    "SuccessWithDifferentTopic",
			addr:    []byte(`{"topic": "orders-topic"}`),
			client:  successClient,
			success: true,
			request: &request{
				topic: "orders-topic",
				body:  []byte("test message"),
			},
		},
		{
			name:    "FailureDueToJson",
			addr:    []byte(""),
			client:  successClient,
			success: false,
		},
		{
			name:    "FailureDueToMissingTopic",
			addr:    []byte(`{}`),
			client:  successClient,
			success: false,
		},
		{
			name:    "FailureDueToClient",
			addr:    []byte(`{"topic": "test-topic"}`),
			client:  failureClient,
			success: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pubsub, err := NewWithClient(nil, metrics, &Config{Size: 1, Workers: 1, Timeout: 1 * time.Second, TimeToRetry: 15 * time.Second, TimeToClaim: 1 * time.Minute}, tc.client)
			assert.Nil(t, err)

			err = pubsub.Start(nil)
			assert.Nil(t, err)

			ok := pubsub.Enqueue(&aio.Message{
				Addr: tc.addr,
				Head: map[string]string{
					"foo": "bar",
					"baz": "qux",
				},
				Body: []byte("test message"),
				Done: func(completion *t_aio.SenderCompletion) {
					assert.Equal(t, tc.success, completion.Success)
				},
			})

			assert.True(t, ok)

			if tc.success {
				assert.Equal(t, tc.request, <-ch)
			}

			err = pubsub.Stop()
			assert.Nil(t, err)
		})
	}
}

func TestPubSubBasics(t *testing.T) {
	metrics := metrics.New(prometheus.NewRegistry())

	t.Run("ConfigValidation", func(t *testing.T) {
		_, err := New(nil, metrics, &Config{
			Size:      10,
			Workers:   2,
			Timeout:   1 * time.Second,
			ProjectID: "",
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "GCP project ID is required")
	})
}

func TestProcessorProcessing(t *testing.T) {
	t.Run("ProcessInvalidJSON", func(t *testing.T) {
		proc := &processor{timeout: 1 * time.Second}
		success, err := proc.Process([]byte("invalid"), nil, []byte("test"))
		assert.False(t, success)
		assert.Error(t, err)
	})

	t.Run("ProcessMissingTopic", func(t *testing.T) {
		proc := &processor{timeout: 1 * time.Second}
		success, err := proc.Process([]byte(`{}`), nil, []byte("test"))
		assert.False(t, success)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "missing topic")
	})

	t.Run("ProcessEmptyTopic", func(t *testing.T) {
		proc := &processor{timeout: 1 * time.Second}
		success, err := proc.Process([]byte(`{"topic":""}`), nil, []byte("test"))
		assert.False(t, success)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "missing topic")
	})

	t.Run("ProcessMalformedJSON", func(t *testing.T) {
		proc := &processor{timeout: 1 * time.Second}
		success, err := proc.Process([]byte(`{"topic":"test"`), nil, []byte("test"))
		assert.False(t, success)
		assert.Error(t, err)
	})
}

func TestPubSubWithNilClient(t *testing.T) {
	metrics := metrics.New(prometheus.NewRegistry())

	pubsub, err := NewWithClient(nil, metrics, &Config{Size: 1, Workers: 1, Timeout: 1 * time.Second, TimeToRetry: 15 * time.Second, TimeToClaim: 1 * time.Minute}, nil)
	assert.Nil(t, err)

	err = pubsub.Stop()
	assert.Nil(t, err)
}

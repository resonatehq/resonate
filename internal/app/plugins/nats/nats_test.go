package nats

import (
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/metrics"
)

type MockNATSClient struct {
	ch chan<- *PublishParams
	ok bool
	// Add a field to control publish behavior
	PublishFunc func(subject string, data []byte) error
}

type PublishParams struct {
	Subject string
	Data    []byte
}

func (m *MockNATSClient) Publish(subject string, data []byte) error {
	if m.PublishFunc != nil {
		return m.PublishFunc(subject, data)
	}

	if !m.ok {
		return errors.New("mock error: failed to publish message")
	}

	if m.ch != nil {
		select {
		case m.ch <- &PublishParams{
			Subject: subject,
			Data:    data,
		}:
		default:
			// Channel is full or no one is reading, ignore
		}
	}

	return nil
}

func (m *MockNATSClient) PublishRequest(subject, reply string, data []byte) error {
	if !m.ok {
		return errors.New("mock error: failed to publish request")
	}

	if m.ch != nil {
		select {
		case m.ch <- &PublishParams{
			Subject: subject,
			Data:    data,
		}:
		default:
			// Channel is full or no one is reading, ignore
		}
	}

	return nil
}

func (m *MockNATSClient) Close() {}

func TestNATSPlugin(t *testing.T) {
	metrics := metrics.New(prometheus.NewRegistry())

	ch := make(chan *PublishParams, 1)

	success_client := &MockNATSClient{ch: ch, ok: true}
	failure_client := &MockNATSClient{ch: ch, ok: false}

	for _, tc := range []struct {
		name    string
		addr    []byte
		client  *MockNATSClient
		success bool
		params  *PublishParams
	}{
		{
			name:    "Success",
			addr:    []byte(`{"url": "nats://localhost:4222", "subject": "test.subject"}`),
			client:  success_client,
			success: true,
			params: &PublishParams{
				Subject: "test.subject",
				Data:    []byte("test message"),
			},
		},
		{
			name:    "SuccessWithDifferentSubject",
			addr:    []byte(`{"url": "nats://localhost:4222", "subject": "my.custom.subject"}`),
			client:  success_client,
			success: true,
			params: &PublishParams{
				Subject: "my.custom.subject",
				Data:    []byte("test message"),
			},
		},
		{
			name:    "SuccessWithDifferentURL",
			addr:    []byte(`{"url": "nats://example.com:4222", "subject": "test.subject"}`),
			client:  success_client,
			success: true,
			params: &PublishParams{
				Subject: "test.subject",
				Data:    []byte("test message"),
			},
		},
		{
			name:    "SuccessWithComplexSubject",
			addr:    []byte(`{"url": "nats://localhost:4222", "subject": "orders.created.v1"}`),
			client:  success_client,
			success: true,
			params: &PublishParams{
				Subject: "orders.created.v1",
				Data:    []byte("test message"),
			},
		},
		{
			name:    "SuccessWithSecureURL",
			addr:    []byte(`{"url": "nats://secure.example.com:4222", "subject": "secure.topic"}`),
			client:  success_client,
			success: true,
			params: &PublishParams{
				Subject: "secure.topic",
				Data:    []byte("test message"),
			},
		},
		{
			name:    "FailureDueToJson",
			addr:    []byte(""),
			client:  success_client,
			success: false,
		},
		{
			name:    "FailureDueToClient",
			addr:    []byte(`{"url": "nats://localhost:4222", "subject": "test.subject"}`),
			client:  failure_client,
			success: false,
		},
		{
			name:    "FailureDueToMissingURL",
			addr:    []byte(`{"subject": "test.subject"}`),
			client:  success_client,
			success: false,
		},
		{
			name:    "FailureDueToMissingSubject",
			addr:    []byte(`{"url": "nats://localhost:4222"}`),
			client:  success_client,
			success: false,
		},
		{
			name:    "FailureDueToEmptyURL",
			addr:    []byte(`{"url": "", "subject": "test.subject"}`),
			client:  success_client,
			success: false,
		},
		{
			name:    "FailureDueToEmptySubject",
			addr:    []byte(`{"url": "nats://localhost:4222", "subject": ""}`),
			client:  success_client,
			success: false,
		},
		{
			name:    "FailureDueToMalformedJSON",
			addr:    []byte(`{"url": "nats://localhost:4222", "subject": "test.subject"`),
			client:  success_client,
			success: false,
		},
		{
			name:    "FailureDueToInvalidJSON",
			addr:    []byte(`{"url": "nats://localhost:4222", "subject": "test.subject", "extra": }`),
			client:  success_client,
			success: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nats, err := NewWithClient(nil, metrics, &Config{Size: 1, Workers: 1}, tc.client)
			assert.Nil(t, err)

			err = nats.Start(nil)
			assert.Nil(t, err)

			done := make(chan bool, 1)
			ok := nats.Enqueue(&aio.Message{
				Addr: tc.addr,
				Body: []byte("test message"),
				Done: func(completion *t_aio.SenderCompletion) {
					assert.Equal(t, tc.success, completion.Success)
					done <- true
				},
			})

			assert.True(t, ok)

			select {
			case <-done:
				// Test completed
			case <-time.After(2 * time.Second):
				t.Fatal("Test timed out")
			}

			if tc.success {
				select {
				case params := <-ch:
					assert.Equal(t, tc.params, params)
				case <-time.After(100 * time.Millisecond):
					// No params expected for failure cases
				}
			}

			err = nats.Stop()
			assert.Nil(t, err)
		})
	}

	close(ch)
}

func TestNATSErrorHandling(t *testing.T) {
	metrics := metrics.New(prometheus.NewRegistry())

	t.Run("ErrorPropagation", func(t *testing.T) {
		ch := make(chan *PublishParams, 1)
		defer close(ch)

		error_client := &MockNATSClient{ch: ch, ok: false}

		nats, err := NewWithClient(nil, metrics, &Config{Size: 1, Workers: 1}, error_client)
		assert.Nil(t, err)

		err = nats.Start(nil)
		assert.Nil(t, err)

		addr := []byte(`{"url": "nats://localhost:4222", "subject": "test.subject"}`)

		done := make(chan bool, 1)
		ok := nats.Enqueue(&aio.Message{
			Addr: addr,
			Body: []byte("test message"),
			Done: func(completion *t_aio.SenderCompletion) {
				assert.False(t, completion.Success)
				done <- true
			},
		})

		assert.True(t, ok)

		select {
		case <-done:
			// Test completed
		case <-time.After(1 * time.Second):
			t.Fatal("Test timed out")
		}

		err = nats.Stop()
		assert.Nil(t, err)
	})

	t.Run("MalformedJSON", func(t *testing.T) {
		ch := make(chan *PublishParams, 1)
		defer close(ch)

		success_client := &MockNATSClient{ch: ch, ok: true}

		nats, err := NewWithClient(nil, metrics, &Config{Size: 1, Workers: 1}, success_client)
		assert.Nil(t, err)

		err = nats.Start(nil)
		assert.Nil(t, err)

		addr := []byte(`{"url": "nats://localhost:4222", "subject": "test.subject"`)

		done := make(chan bool, 1)
		ok := nats.Enqueue(&aio.Message{
			Addr: addr,
			Body: []byte("test message"),
			Done: func(completion *t_aio.SenderCompletion) {
				assert.False(t, completion.Success)
				done <- true
			},
		})

		assert.True(t, ok)

		select {
		case <-done:
			// Test completed
		case <-time.After(1 * time.Second):
			t.Fatal("Test timed out")
		}

		err = nats.Stop()
		assert.Nil(t, err)
	})

	t.Run("ConcurrentProcessing", func(t *testing.T) {
		ch := make(chan *PublishParams, 10)
		defer close(ch)

		success_client := &MockNATSClient{ch: ch, ok: true}

		nats, err := NewWithClient(nil, metrics, &Config{Size: 10, Workers: 2}, success_client)
		assert.Nil(t, err)

		err = nats.Start(nil)
		assert.Nil(t, err)

		num_messages := 5
		done := make(chan bool, num_messages)

		for i := 0; i < num_messages; i++ {
			addr := []byte(`{"url": "nats://localhost:4222", "subject": "test.subject"}`)
			ok := nats.Enqueue(&aio.Message{
				Addr: addr,
				Body: []byte("test message"),
				Done: func(completion *t_aio.SenderCompletion) {
					assert.True(t, completion.Success)
					done <- true
				},
			})
			assert.True(t, ok)
		}

		for i := 0; i < num_messages; i++ {
			select {
			case <-done:
				// Message completed
			case <-time.After(2 * time.Second):
				t.Fatal("Test timed out waiting for message completion")
			}
		}

		assert.Equal(t, num_messages, len(ch))
		for i := 0; i < num_messages; i++ {
			params := <-ch
			assert.Equal(t, "test.subject", params.Subject)
			assert.Equal(t, []byte("test message"), params.Data)
		}

		err = nats.Stop()
		assert.Nil(t, err)
	})

	t.Run("QueueOverflow", func(t *testing.T) {
		ch := make(chan *PublishParams, 1)
		defer close(ch)

		success_client := &MockNATSClient{ch: ch, ok: true}

		nats, err := NewWithClient(nil, metrics, &Config{Size: 1, Workers: 1}, success_client)
		assert.Nil(t, err)

		err = nats.Start(nil)
		assert.Nil(t, err)

		addr := []byte(`{"url": "nats://localhost:4222", "subject": "test.subject"}`)
		ok := nats.Enqueue(&aio.Message{
			Addr: addr,
			Body: []byte("test message 1"),
			Done: func(completion *t_aio.SenderCompletion) {},
		})
		assert.True(t, ok)

		ok = nats.Enqueue(&aio.Message{
			Addr: addr,
			Body: []byte("test message 2"),
			Done: func(completion *t_aio.SenderCompletion) {},
		})
		assert.False(t, ok)

		err = nats.Stop()
		assert.Nil(t, err)
	})
}

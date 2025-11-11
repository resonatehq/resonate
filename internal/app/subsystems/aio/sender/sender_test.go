package sender

import (
	"testing"

	"github.com/resonatehq/resonate/pkg/receiver"
	"github.com/stretchr/testify/assert"
)

func TestUrlParse(t *testing.T) {
	tests := []struct {
		name string
		url  string
		recv *receiver.Recv
		ok   bool
	}{
		// http/https scheme
		{
			name: "valid http url",
			url:  "http://example.com",
			recv: &receiver.Recv{Type: "http", Data: []byte(`{"url":"http://example.com"}`)},
			ok:   true,
		},
		{
			name: "valid https url",
			url:  "https://example.com",
			recv: &receiver.Recv{Type: "http", Data: []byte(`{"url":"https://example.com"}`)},
			ok:   true,
		},
		// poll scheme
		{
			name: "valid uni poll url",
			url:  "poll://uni@default",
			recv: &receiver.Recv{Type: "poll", Data: []byte(`{"cast":"uni","group":"default"}`)},
			ok:   true,
		},
		{
			name: "valid any poll url",
			url:  "poll://any@default",
			recv: &receiver.Recv{Type: "poll", Data: []byte(`{"cast":"any","group":"default"}`)},
			ok:   true,
		},
		{
			name: "valid uni poll url with id",
			url:  "poll://uni@default/abc",
			recv: &receiver.Recv{Type: "poll", Data: []byte(`{"cast":"uni","group":"default","id":"abc"}`)},
			ok:   true,
		},
		{
			name: "valid any poll url with id",
			url:  "poll://any@default/xyz",
			recv: &receiver.Recv{Type: "poll", Data: []byte(`{"cast":"any","group":"default","id":"xyz"}`)},
			ok:   true,
		},
		{
			name: "cast defaults to any if not provided",
			url:  "poll://default",
			recv: &receiver.Recv{Type: "poll", Data: []byte(`{"cast":"any","group":"default"}`)},
			ok:   true,
		},
		{
			name: "cast defaults to any if not provided (with id)",
			url:  "poll://default/xyz",
			recv: &receiver.Recv{Type: "poll", Data: []byte(`{"cast":"any","group":"default","id":"xyz"}`)},
			ok:   true,
		},
		{
			name: "cast must be one of uni or any",
			url:  "poll://nope@default",
			recv: nil,
			ok:   false,
		},
		// sqs+https scheme
		{
			name: "valid sqs url",
			url:  "sqs+https://sqs.us-west-2.amazonaws.com/123456789012/my-queue",
			recv: &receiver.Recv{Type: "sqs", Data: []byte(`{"url":"https://sqs.us-west-2.amazonaws.com/123456789012/my-queue"}`)},
			ok:   true,
		},
		{
			name: "valid sqs url with nested queue name",
			url:  "sqs+https://sqs.us-east-1.amazonaws.com/123456789012/my-queue/sub-queue",
			recv: &receiver.Recv{Type: "sqs", Data: []byte(`{"url":"https://sqs.us-east-1.amazonaws.com/123456789012/my-queue/sub-queue"}`)},
			ok:   true,
		},
		{
			name: "sqs url with empty host",
			url:  "sqs+https:///my-queue",
			recv: &receiver.Recv{Type: "sqs", Data: []byte(`{"url":"https:///my-queue"}`)},
			ok:   true,
		},
		{
			name: "sqs url with empty path",
			url:  "sqs+https://sqs.us-west-2.amazonaws.com/123456789012/",
			recv: &receiver.Recv{Type: "sqs", Data: []byte(`{"url":"https://sqs.us-west-2.amazonaws.com/123456789012/"}`)},
			ok:   true,
		},
		{
			name: "sqs url with missing path",
			url:  "sqs+https://sqs.us-west-2.amazonaws.com/123456789012",
			recv: &receiver.Recv{Type: "sqs", Data: []byte(`{"url":"https://sqs.us-west-2.amazonaws.com/123456789012"}`)},
			ok:   true,
		},
		{
			name: "sqs url with special characters in queue name",
			url:  "sqs+https://sqs.us-west-2.amazonaws.com/123456789012/my-queue_123-456",
			recv: &receiver.Recv{Type: "sqs", Data: []byte(`{"url":"https://sqs.us-west-2.amazonaws.com/123456789012/my-queue_123-456"}`)},
			ok:   true,
		},
		// pubsub scheme
		{
			name: "valid pubsub url",
			url:  "pubsub://my-project/my-topic",
			recv: &receiver.Recv{Type: "pubsub", Data: []byte(`{"topic":"my-topic"}`)},
			ok:   true,
		},
		{
			name: "valid pubsub url with nested topic name",
			url:  "pubsub://my-project/orders-processing-v1",
			recv: &receiver.Recv{Type: "pubsub", Data: []byte(`{"topic":"orders-processing-v1"}`)},
			ok:   true,
		},
		{
			name: "valid pubsub url with complex topic path",
			url:  "pubsub://my-project/projects/my-project/topics/my-topic",
			recv: &receiver.Recv{Type: "pubsub", Data: []byte(`{"topic":"projects/my-project/topics/my-topic"}`)},
			ok:   true,
		},
		{
			name: "pubsub url with empty topic",
			url:  "pubsub://my-project/",
			recv: nil,
			ok:   false,
		},
		{
			name: "pubsub url with missing topic",
			url:  "pubsub://my-project",
			recv: nil,
			ok:   false,
		},
		// unsupported scheme
		{
			name: "scheme must be http, https, poll, sqs+https, nats, or pubsub",
			url:  "nope://example.com",
			recv: nil,
			ok:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recv, ok := schemeToRecv(tt.url)
			assert.Equal(t, tt.recv, recv)
			assert.Equal(t, tt.ok, ok)
		})
	}
}

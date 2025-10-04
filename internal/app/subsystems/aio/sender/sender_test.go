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
		// nats scheme
		{
			name: "valid nats url",
			url:  "nats://localhost/my-queue",
			recv: &receiver.Recv{Type: "nats", Data: []byte(`{"subject":"my-queue","url":"nats://localhost/my-queue"}`)},
			ok:   true,
		},
		{
			name: "valid nats url with custom port",
			url:  "nats://localhost:4222/my-queue",
			recv: &receiver.Recv{Type: "nats", Data: []byte(`{"subject":"my-queue","url":"nats://localhost:4222/my-queue"}`)},
			ok:   true,
		},
		{
			name: "valid nats url with nested subject name",
			url:  "nats://localhost:4222/orders.processing.priority",
			recv: &receiver.Recv{Type: "nats", Data: []byte(`{"subject":"orders.processing.priority","url":"nats://localhost:4222/orders.processing.priority"}`)},
			ok:   true,
		},
		{
			name: "valid nats url with queue group subject",
			url:  "nats://localhost:4222/workers.queue",
			recv: &receiver.Recv{Type: "nats", Data: []byte(`{"subject":"workers.queue","url":"nats://localhost:4222/workers.queue"}`)},
			ok:   true,
		},
		{
			name: "valid nats url with special characters in subject name",
			url:  "nats://localhost:4222/my-queue_123-456",
			recv: &receiver.Recv{Type: "nats", Data: []byte(`{"subject":"my-queue_123-456","url":"nats://localhost:4222/my-queue_123-456"}`)},
			ok:   true,
		},
		{
			name: "valid nats url with multiple servers",
			url:  "nats://nats1:4222,nats2:4222/my-queue",
			recv: &receiver.Recv{Type: "nats", Data: []byte(`{"subject":"my-queue","url":"nats://nats1:4222,nats2:4222/my-queue"}`)},
			ok:   true,
		},
		{
			name: "valid nats url with authentication",
			url:  "nats://user:pass@localhost:4222/my-queue",
			recv: &receiver.Recv{Type: "nats", Data: []byte(`{"subject":"my-queue","url":"nats://user:pass@localhost:4222/my-queue"}`)},
			ok:   true,
		},
		{
			name: "nats url with empty subject",
			url:  "nats://localhost:4222/",
			recv: nil,
			ok:   false,
		},
		{
			name: "nats url with missing subject",
			url:  "nats://localhost:4222",
			recv: nil,
			ok:   false,
		},
		{
			name: "nats url with invalid port",
			url:  "nats://localhost:99999/my-queue",
			recv: &receiver.Recv{Type: "nats", Data: []byte(`{"subject":"my-queue","url":"nats://localhost:99999/my-queue"}`)},
			ok:   true,
		},
		{
			name: "nats url with wildcard subject",
			url:  "nats://localhost:4222/orders.*",
			recv: &receiver.Recv{Type: "nats", Data: []byte(`{"subject":"orders.*","url":"nats://localhost:4222/orders.*"}`)},
			ok:   true,
		},
		{
			name: "nats url with multi-level wildcard subject",
			url:  "nats://localhost:4222/orders.>",
			recv: &receiver.Recv{Type: "nats", Data: []byte(`{"subject":"orders.\u003e","url":"nats://localhost:4222/orders.%3E"}`)},
			ok:   true,
		},
		{
			name: "nats url with subject containing dots and slashes",
			url:  "nats://localhost:4222/api/v1/orders.process",
			recv: &receiver.Recv{Type: "nats", Data: []byte(`{"subject":"api/v1/orders.process","url":"nats://localhost:4222/api/v1/orders.process"}`)},
			ok:   true,
		},
		// unsupported scheme
		{
			name: "scheme must be http, https, poll, or sqs+https",
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

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
		// kafka scheme
		{
			name: "valid kafka url",
			url:  "kafka://my-topic",
			recv: &receiver.Recv{Type: "kafka", Data: []byte(`{"topic":"my-topic"}`)},
			ok:   true,
		},
		{
			name: "valid kafka url with key",
			url:  "kafka://my-topic?key=partition-key",
			recv: &receiver.Recv{Type: "kafka", Data: []byte(`{"key":"partition-key","topic":"my-topic"}`)},
			ok:   true,
		},
		{
			name: "valid kafka url with underscore in topic",
			url:  "kafka://my_topic_123",
			recv: &receiver.Recv{Type: "kafka", Data: []byte(`{"topic":"my_topic_123"}`)},
			ok:   true,
		},
		{
			name: "valid kafka url with hyphen in topic",
			url:  "kafka://my-topic-123",
			recv: &receiver.Recv{Type: "kafka", Data: []byte(`{"topic":"my-topic-123"}`)},
			ok:   true,
		},
		{
			name: "valid kafka url with numbers in topic",
			url:  "kafka://topic123",
			recv: &receiver.Recv{Type: "kafka", Data: []byte(`{"topic":"topic123"}`)},
			ok:   true,
		},
		{
			name: "valid kafka url with dot in topic",
			url:  "kafka://my.topic.name",
			recv: &receiver.Recv{Type: "kafka", Data: []byte(`{"topic":"my.topic.name"}`)},
			ok:   true,
		},
		{
			name: "kafka url with empty topic",
			url:  "kafka://",
			recv: nil,
			ok:   false,
		},
		{
			name: "kafka url with empty host",
			url:  "kafka:///",
			recv: nil,
			ok:   false,
		},
		{
			name: "kafka url with query params but no key",
			url:  "kafka://my-topic?other=value",
			recv: &receiver.Recv{Type: "kafka", Data: []byte(`{"topic":"my-topic"}`)},
			ok:   true,
		},
		{
			name: "kafka url with multiple query params",
			url:  "kafka://my-topic?key=partition-key&other=value",
			recv: &receiver.Recv{Type: "kafka", Data: []byte(`{"key":"partition-key","topic":"my-topic"}`)},
			ok:   true,
		},
		{
			name: "kafka url with URL encoded key",
			url:  "kafka://my-topic?key=partition%20key",
			recv: &receiver.Recv{Type: "kafka", Data: []byte(`{"key":"partition key","topic":"my-topic"}`)},
			ok:   true,
		},
		// nats scheme
		{
			name: "valid nats url",
			url:  "nats://my-subject",
			recv: &receiver.Recv{Type: "nats", Data: []byte(`{"subject":"my-subject"}`)},
			ok:   true,
		},
		{
			name: "valid nats url with dots in subject",
			url:  "nats://orders.created.v1",
			recv: &receiver.Recv{Type: "nats", Data: []byte(`{"subject":"orders.created.v1"}`)},
			ok:   true,
		},
		{
			name: "valid nats url with underscores in subject",
			url:  "nats://my_subject_123",
			recv: &receiver.Recv{Type: "nats", Data: []byte(`{"subject":"my_subject_123"}`)},
			ok:   true,
		},
		{
			name: "valid nats url with hyphens in subject",
			url:  "nats://my-subject-123",
			recv: &receiver.Recv{Type: "nats", Data: []byte(`{"subject":"my-subject-123"}`)},
			ok:   true,
		},
		{
			name: "nats url with empty subject",
			url:  "nats://",
			recv: nil,
			ok:   false,
		},
		{
			name: "nats url with empty host (slash path)",
			url:  "nats:///",
			recv: nil,
			ok:   false,
		},
		// unsupported scheme
		{
			name: "scheme must be http, https, poll, sqs+https, kafka, or nats",
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

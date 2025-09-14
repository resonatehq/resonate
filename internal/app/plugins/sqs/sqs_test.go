package sqs

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/metrics"
)

type MockSQSClient struct {
	fail_send bool
}

func (m *MockSQSClient) SendMessage(
	ctx context.Context, params *sqs.SendMessageInput, opt ...func(*sqs.Options),
) (*sqs.SendMessageOutput, error) {
	if m.fail_send {
		return nil, errors.New("mock error: failed to send message")
	}

	message_id := "test-message-id"
	return &sqs.SendMessageOutput{
		MessageId: &message_id,
	}, nil
}

func TestSQSPlugin(t *testing.T) {
	metrics := metrics.New(prometheus.NewRegistry())

	for _, tc := range []struct {
		name    string
		data    *Addr
		success bool
	}{
		{
			name: "valid",
			data: &Addr{
				QueueURL: "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue",
			},
			success: true,
		},
		{
			name: "missing queue_url",
			data: &Addr{
				QueueURL: "",
			},
			success: false,
		},
		{
			name: "invalid SQS URL format",
			data: &Addr{
				QueueURL: "https://invalid-url.com/queue",
			},
			success: false,
		},
		{
			name: "invalid SQS URL - missing sqs prefix",
			data: &Addr{
				QueueURL: "https://us-west-2.amazonaws.com/123456789012/test-queue",
			},
			success: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock_client := &MockSQSClient{
				fail_send: false,
			}

			sqsPlugin, err := NewWithClient(nil, metrics, &Config{Size: 1, Workers: 1, Timeout: 1 * time.Second}, mock_client)
			assert.Nil(t, err)

			data, err := json.Marshal(tc.data)
			assert.Nil(t, err)

			err = sqsPlugin.Start(nil)
			assert.Nil(t, err)

			ok := sqsPlugin.Enqueue(&aio.Message{
				Addr: data,
				Body: []byte("test message"),
				Done: func(success bool, err error) {
					if tc.success {
						assert.Nil(t, err)
						assert.True(t, success)
					} else {
						assert.NotNil(t, err)
						assert.False(t, success)
					}
				},
			})

			assert.True(t, ok)
			time.Sleep(100 * time.Millisecond)

			err = sqsPlugin.Stop()
			assert.Nil(t, err)
		})
	}
}

func TestSQSPluginSchemaValidation(t *testing.T) {
	metrics := metrics.New(prometheus.NewRegistry())

	for _, tc := range []struct {
		name         string
		addr_data    string
		expect_error bool
		error_msg    string
	}{
		{
			name:         "valid JSON with queue_url",
			addr_data:    `{"queue_url": "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue"}`,
			expect_error: false,
		},
		{
			name:         "valid JSON with whitespace",
			addr_data:    `{"queue_url": " https://sqs.us-west-2.amazonaws.com/123456789012/test-queue "}`,
			expect_error: false,
		},
		{
			name:         "malformed JSON - missing closing brace",
			addr_data:    `{"queue_url": "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue"`,
			expect_error: true,
			error_msg:    "unexpected end of JSON input",
		},
		{
			name:         "malformed JSON - invalid syntax",
			addr_data:    `{"queue_url": "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue",}`,
			expect_error: true,
			error_msg:    "invalid character",
		},
		{
			name:         "empty JSON object",
			addr_data:    `{}`,
			expect_error: true,
			error_msg:    "missing queue_url in address",
		},
		{
			name:         "null values",
			addr_data:    `{"queue_url": null}`,
			expect_error: true,
			error_msg:    "missing queue_url in address",
		},
		{
			name:         "non-string queue_url",
			addr_data:    `{"queue_url": 123}`,
			expect_error: true,
			error_msg:    "cannot unmarshal number into Go struct field Addr.queue_url of type string",
		},
		{
			name:         "array instead of object",
			addr_data:    `["https://sqs.us-west-2.amazonaws.com/123456789012/test-queue"]`,
			expect_error: true,
			error_msg:    "json: cannot unmarshal array",
		},
		{
			name:         "string instead of object",
			addr_data:    `"invalid"`,
			expect_error: true,
			error_msg:    "json: cannot unmarshal string",
		},
		{
			name:         "number instead of object",
			addr_data:    `123`,
			expect_error: true,
			error_msg:    "json: cannot unmarshal number",
		},
		{
			name:         "boolean instead of object",
			addr_data:    `true`,
			expect_error: true,
			error_msg:    "json: cannot unmarshal bool",
		},
		{
			name:         "empty string",
			addr_data:    ``,
			expect_error: true,
			error_msg:    "unexpected end of JSON input",
		},
		{
			name:         "extra fields ignored",
			addr_data:    `{"queue_url": "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue", "extra": "ignored"}`,
			expect_error: false,
		},
		{
			name:         "case insensitive field names (Go JSON unmarshaling is case insensitive)",
			addr_data:    `{"Queue_URL": "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue"}`,
			expect_error: false,
		},
		{
			name:         "invalid SQS URL format",
			addr_data:    `{"queue_url": "https://invalid-url.com/queue"}`,
			expect_error: true,
			error_msg:    "failed to parse SQS URL",
		},
		{
			name:         "very long queue URL",
			addr_data:    `{"queue_url": "https://sqs.us-west-2.amazonaws.com/123456789012/` + strings.Repeat("b", 1000) + `"}`,
			expect_error: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockClient := &MockSQSClient{
				fail_send: false,
			}

			sqsPlugin, err := NewWithClient(nil, metrics, &Config{Size: 1, Workers: 1, Timeout: 1 * time.Second}, mockClient)
			assert.Nil(t, err)

			err = sqsPlugin.Start(nil)
			assert.Nil(t, err)

			var actual_error error
			ok := sqsPlugin.Enqueue(&aio.Message{
				Addr: []byte(tc.addr_data),
				Body: []byte("test message"),
				Done: func(success bool, err error) {
					actual_error = err
					if tc.expect_error {
						assert.False(t, success)
						assert.NotNil(t, err)
						if tc.error_msg != "" && err != nil {
							assert.Contains(t, err.Error(), tc.error_msg)
						}
					} else {
						assert.True(t, success)
						assert.Nil(t, err)
					}
				},
			})

			assert.True(t, ok)
			time.Sleep(100 * time.Millisecond)

			if tc.expect_error {
				assert.NotNil(t, actual_error)
				if tc.error_msg != "" && actual_error != nil {
					assert.Contains(t, actual_error.Error(), tc.error_msg)
				}
			} else {
				assert.Nil(t, actual_error)
			}

			err = sqsPlugin.Stop()
			assert.Nil(t, err)
		})
	}
}

func TestSQSPluginAWSClientErrors(t *testing.T) {
	metrics := metrics.New(prometheus.NewRegistry())

	for _, tc := range []struct {
		name                    string
		fail_send               bool
		expect_error            bool
		expected_error_contains string
	}{
		{
			name:                    "SendMessage fails",
			fail_send:               true,
			expect_error:            true,
			expected_error_contains: "failed to send message",
		},
		{
			name:         "SendMessage succeeds",
			fail_send:    false,
			expect_error: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockClient := &MockSQSClient{
				fail_send: tc.fail_send,
			}

			sqsPlugin, err := NewWithClient(nil, metrics, &Config{Size: 1, Workers: 1, Timeout: 1 * time.Second}, mockClient)
			assert.Nil(t, err)

			valid_addr := &Addr{
				QueueURL: "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue",
			}
			data, err := json.Marshal(valid_addr)
			assert.Nil(t, err)

			err = sqsPlugin.Start(nil)
			assert.Nil(t, err)

			var actual_error error
			ok := sqsPlugin.Enqueue(&aio.Message{
				Addr: data,
				Body: []byte("test message"),
				Done: func(success bool, err error) {
					actual_error = err
					if tc.expect_error {
						assert.False(t, success)
						assert.NotNil(t, err)
						assert.Contains(t, err.Error(), tc.expected_error_contains)
					} else {
						assert.True(t, success)
						assert.Nil(t, err)
					}
				},
			})

			assert.True(t, ok)
			time.Sleep(100 * time.Millisecond)

			if tc.expect_error {
				assert.NotNil(t, actual_error)
				assert.Contains(t, actual_error.Error(), tc.expected_error_contains)
			} else {
				assert.Nil(t, actual_error)
			}

			err = sqsPlugin.Stop()
			assert.Nil(t, err)
		})
	}
}

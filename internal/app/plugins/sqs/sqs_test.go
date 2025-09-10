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
	fail_queue bool
	fail_send  bool
}

func (m *MockSQSClient) GetQueueUrl(ctx context.Context, params *sqs.GetQueueUrlInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
	if m.fail_queue {
		return nil, errors.New("mock error: failed to get queue URL")
	}

	if params.QueueName == nil || *params.QueueName == "" {
		return nil, errors.New("mock error: queue name cannot be empty")
	}

	queue_url := "https://sqs.us-west-2.amazonaws.com/123456789012/" + *params.QueueName
	return &sqs.GetQueueUrlOutput{
		QueueUrl: &queue_url,
	}, nil
}

func (m *MockSQSClient) SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
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
				Region: "us-west-2",
				Queue:  "test-queue",
			},
			success: true,
		},
		{
			name: "missing region",
			data: &Addr{
				Region: "",
				Queue:  "test-queue",
			},
			success: false,
		},
		{
			name: "missing queue",
			data: &Addr{
				Region: "us-west-2",
				Queue:  "",
			},
			success: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock_client := &MockSQSClient{
				fail_queue: false,
				fail_send:  false,
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
			name:         "valid JSON with both fields",
			addr_data:    `{"region": "us-west-2", "queue": "test-queue"}`,
			expect_error: false,
		},
		{
			name:         "valid JSON with whitespace",
			addr_data:    `{"region": " us-west-2 ", "queue": " test-queue "}`,
			expect_error: false,
		},
		{
			name:         "malformed JSON - missing closing brace",
			addr_data:    `{"region": "us-west-2", "queue": "test-queue"`,
			expect_error: true,
			error_msg:    "unexpected end of JSON input",
		},
		{
			name:         "malformed JSON - invalid syntax",
			addr_data:    `{"region": "us-west-2", "queue": "test-queue",}`,
			expect_error: true,
			error_msg:    "invalid character",
		},
		{
			name:         "empty JSON object",
			addr_data:    `{}`,
			expect_error: true,
			error_msg:    "missing region in address",
		},
		{
			name:         "null values",
			addr_data:    `{"region": null, "queue": null}`,
			expect_error: true,
			error_msg:    "missing region in address",
		},
		{
			name:         "non-string region",
			addr_data:    `{"region": 123, "queue": "test-queue"}`,
			expect_error: true,
			error_msg:    "cannot unmarshal number into Go struct field Addr.region of type string",
		},
		{
			name:         "non-string queue",
			addr_data:    `{"region": "us-west-2", "queue": 456}`,
			expect_error: true,
			error_msg:    "cannot unmarshal number into Go struct field Addr.queue of type string",
		},
		{
			name:         "array instead of object",
			addr_data:    `["us-west-2", "test-queue"]`,
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
			addr_data:    `{"region": "us-west-2", "queue": "test-queue", "extra": "ignored"}`,
			expect_error: false,
		},
		{
			name:         "case insensitive field names (Go JSON unmarshaling is case insensitive)",
			addr_data:    `{"Region": "us-west-2", "Queue": "test-queue"}`,
			expect_error: false,
		},
		{
			name:         "special characters in region",
			addr_data:    `{"region": "us-west-2@#$", "queue": "test-queue"}`,
			expect_error: false,
		},
		{
			name:         "special characters in queue",
			addr_data:    `{"region": "us-west-2", "queue": "test-queue_123-456"}`,
			expect_error: false,
		},
		{
			name:         "very long region name",
			addr_data:    `{"region": "` + strings.Repeat("a", 1000) + `", "queue": "test-queue"}`,
			expect_error: false,
		},
		{
			name:         "very long queue name",
			addr_data:    `{"region": "us-west-2", "queue": "` + strings.Repeat("b", 1000) + `"}`,
			expect_error: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockClient := &MockSQSClient{
				fail_queue: false,
				fail_send:  false,
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
		fail_queue              bool
		fail_send               bool
		expect_error            bool
		expected_error_contains string
	}{
		{
			name:                    "GetQueueUrl fails",
			fail_queue:              true,
			expect_error:            true,
			expected_error_contains: "failed to get queue URL",
		},
		{
			name:                    "SendMessage fails",
			fail_send:               true,
			expect_error:            true,
			expected_error_contains: "failed to send message",
		},
		{
			name:         "both operations succeed",
			fail_queue:   false,
			fail_send:    false,
			expect_error: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockClient := &MockSQSClient{
				fail_queue: tc.fail_queue,
				fail_send:  tc.fail_send,
			}

			sqsPlugin, err := NewWithClient(nil, metrics, &Config{Size: 1, Workers: 1, Timeout: 1 * time.Second}, mockClient)
			assert.Nil(t, err)

			valid_addr := &Addr{
				Region: "us-west-2",
				Queue:  "test-queue",
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

package sqs

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/metrics"
)

// mockSQSClient is a mock implementation of SQSClient for testing
type mockSQSClient struct {
	shouldFailGetQueueUrl bool
	shouldFailSendMessage bool
}

func (m *mockSQSClient) GetQueueUrl(ctx context.Context, params *sqs.GetQueueUrlInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
	if m.shouldFailGetQueueUrl {
		return nil, errors.New("mock error: failed to get queue URL")
	}

	queueUrl := "https://sqs.us-west-2.amazonaws.com/123456789012/" + *params.QueueName
	return &sqs.GetQueueUrlOutput{
		QueueUrl: &queueUrl,
	}, nil
}

func (m *mockSQSClient) SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	if m.shouldFailSendMessage {
		return nil, errors.New("mock error: failed to send message")
	}

	messageId := "test-message-id"
	return &sqs.SendMessageOutput{
		MessageId: &messageId,
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
			// Create a mock SQS client that succeeds for valid test cases
			mockClient := &mockSQSClient{
				shouldFailGetQueueUrl: false,
				shouldFailSendMessage: false,
			}

			sqsPlugin, err := NewWithClient(nil, metrics, &Config{Size: 1, Workers: 1, Timeout: 1 * time.Second}, mockClient)
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

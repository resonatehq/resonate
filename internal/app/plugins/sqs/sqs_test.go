package sqs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/metrics"
)

type MockSQSClient struct {
	ch chan<- *SendMessageParams
	ok bool
}

type SendMessageParams struct {
	Url    string
	Region string
}

func (m *MockSQSClient) SendMessage(
	ctx context.Context, params *sqs.SendMessageInput, opt ...func(*sqs.Options),
) (*sqs.SendMessageOutput, error) {
	if !m.ok {
		return nil, errors.New("mock error: failed to send message")
	}

	opts := &sqs.Options{}
	for _, o := range opt {
		o(opts)
	}

	m.ch <- &SendMessageParams{
		Url:    *params.QueueUrl,
		Region: opts.Region,
	}

	id := "test-message-id"
	return &sqs.SendMessageOutput{MessageId: &id}, nil
}

func TestSQSPlugin(t *testing.T) {
	metrics := metrics.New(prometheus.NewRegistry())

	// create a backchannel
	ch := make(chan *SendMessageParams, 1)
	defer close(ch)

	successClient := &MockSQSClient{ch, true}
	failureClient := &MockSQSClient{ch, false}

	for _, tc := range []struct {
		name    string
		addr    []byte
		client  *MockSQSClient
		success bool
		params  *SendMessageParams
	}{
		{
			name:    "Success",
			addr:    []byte(`{"url": "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue"}`),
			client:  successClient,
			success: true,
			params: &SendMessageParams{
				Url:    "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue",
				Region: "us-west-2",
			},
		},
		{
			name:    "SuccessNoRegion",
			addr:    []byte(`{"url": "https://notvalid.com"}`),
			client:  successClient,
			success: true,
			params: &SendMessageParams{
				Url:    "https://notvalid.com",
				Region: "",
			},
		},
		{
			name:    "SuccessRegionTaskPrecedenceOverUrl",
			addr:    []byte(`{"url": "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue", "region": "us-east-2"}`),
			client:  successClient,
			success: true,
			params: &SendMessageParams{
				Url:    "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue",
				Region: "us-east-2",
			},
		},
		{
			name:    "FailureDueToJson",
			addr:    []byte(""),
			client:  successClient,
			success: false,
		},
		{
			name:    "FailureDueToCleint",
			addr:    []byte(`{}`),
			client:  failureClient,
			success: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sqs, err := NewWithClient(nil, metrics, &Config{Size: 1, Workers: 1, Timeout: 30 * time.Second, TimeToRetry: 15 * time.Second, TimeToClaim: 0}, tc.client)
			assert.Nil(t, err)

			err = sqs.Start(nil)
			assert.Nil(t, err)

			ok := sqs.Enqueue(&aio.Message{
				Addr: tc.addr,
				Body: []byte("test message"),
				Done: func(completion *t_aio.SenderCompletion) {
					assert.Equal(t, tc.success, completion.Success)
				},
			})

			assert.True(t, ok)

			if tc.success {
				assert.Equal(t, tc.params, <-ch)
			}

			err = sqs.Stop()
			assert.Nil(t, err)
		})
	}
}

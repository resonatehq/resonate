package sqs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	awstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/metrics"
)

type MockSQSClient struct {
	ch chan<- *request
	ok bool
}

type request struct {
	url    string
	region string
	attr   map[string]awstypes.MessageAttributeValue
	body   string
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

	m.ch <- &request{
		url:    *params.QueueUrl,
		region: opts.Region,
		attr:   params.MessageAttributes,
		body:   *params.MessageBody,
	}

	id := "test-message-id"
	return &sqs.SendMessageOutput{MessageId: &id}, nil
}

func TestSQSPlugin(t *testing.T) {
	metrics := metrics.New(prometheus.NewRegistry())

	// create a backchannel
	ch := make(chan *request, 1)
	defer close(ch)

	successClient := &MockSQSClient{ch, true}
	failureClient := &MockSQSClient{ch, false}

	for _, tc := range []struct {
		name    string
		addr    []byte
		client  *MockSQSClient
		success bool
		request *request
	}{
		{
			name:    "Success",
			addr:    []byte(`{"url": "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue"}`),
			client:  successClient,
			success: true,
			request: &request{
				url:    "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue",
				region: "us-west-2",
				attr: map[string]awstypes.MessageAttributeValue{
					"foo": {DataType: aws.String("String"), StringValue: aws.String("bar")},
					"baz": {DataType: aws.String("String"), StringValue: aws.String("qux")},
				},
				body: "test message",
			},
		},
		{
			name:    "SuccessNoRegion",
			addr:    []byte(`{"url": "https://notvalid.com"}`),
			client:  successClient,
			success: true,
			request: &request{
				url:    "https://notvalid.com",
				region: "",
				attr: map[string]awstypes.MessageAttributeValue{
					"foo": {DataType: aws.String("String"), StringValue: aws.String("bar")},
					"baz": {DataType: aws.String("String"), StringValue: aws.String("qux")},
				},
				body: "test message",
			},
		},
		{
			name:    "SuccessRegionTaskPrecedenceOverUrl",
			addr:    []byte(`{"url": "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue", "region": "us-east-2"}`),
			client:  successClient,
			success: true,
			request: &request{
				url:    "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue",
				region: "us-east-2",
				attr: map[string]awstypes.MessageAttributeValue{
					"foo": {DataType: aws.String("String"), StringValue: aws.String("bar")},
					"baz": {DataType: aws.String("String"), StringValue: aws.String("qux")},
				},
				body: "test message",
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

			err = sqs.Stop()
			assert.Nil(t, err)
		})
	}
}

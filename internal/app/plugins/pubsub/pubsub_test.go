package pubsub

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"

	"github.com/resonatehq/resonate/internal/metrics"
)

func TestPubSubPlugin(t *testing.T) {
	tests := []struct {
		name           string
		addr           []byte
		expect_success bool
		expected_topic string
	}{
		{
			name:           "Success",
			addr:           []byte(`{"topic": "test-topic"}`),
			expect_success: false,
			expected_topic: "test-topic",
		},
		{
			name:           "SuccessWithDifferentTopic",
			addr:           []byte(`{"topic": "orders-topic"}`),
			expect_success: false,
			expected_topic: "orders-topic",
		},
		{
			name:           "SuccessWithComplexTopic",
			addr:           []byte(`{"topic": "projects/my-project/topics/my-topic"}`),
			expect_success: false,
			expected_topic: "projects/my-project/topics/my-topic",
		},
		{
			name:           "FailureDueToJson",
			addr:           []byte(""),
			expect_success: false,
		},
		{
			name:           "FailureDueToMissingTopic",
			addr:           []byte(`{}`),
			expect_success: false,
		},
		{
			name:           "FailureDueToEmptyTopic",
			addr:           []byte(`{"topic": ""}`),
			expect_success: false,
		},
		{
			name:           "FailureDueToMalformedJSON",
			addr:           []byte(`{"topic": "test-topic"`),
			expect_success: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var addr Addr
			err := json.Unmarshal(tt.addr, &addr)

			if tt.expected_topic != "" {
				assert.Nil(t, err, "JSON should parse correctly")
				assert.Equal(t, tt.expected_topic, addr.Topic)
			} else {
				if err == nil {
					assert.Empty(t, addr.Topic, "Topic should be empty for invalid cases")
				}
			}
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
		success, err := proc.Process([]byte("test"), []byte("invalid"))
		assert.False(t, success)
		assert.Error(t, err)
	})

	t.Run("ProcessMissingTopic", func(t *testing.T) {
		proc := &processor{timeout: 1 * time.Second}
		success, err := proc.Process([]byte("test"), []byte(`{}`))
		assert.False(t, success)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "missing topic")
	})

	t.Run("ProcessEmptyTopic", func(t *testing.T) {
		proc := &processor{timeout: 1 * time.Second}
		success, err := proc.Process([]byte("test"), []byte(`{"topic":""}`))
		assert.False(t, success)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "missing topic")
	})

	t.Run("ProcessMalformedJSON", func(t *testing.T) {
		proc := &processor{timeout: 1 * time.Second}
		success, err := proc.Process([]byte("test"), []byte(`{"topic":"test"`))
		assert.False(t, success)
		assert.Error(t, err)
	})
}

func TestCompletionValues(t *testing.T) {
	config := &Config{
		TimeToRetry: 15 * time.Second,
		TimeToClaim: 60 * time.Second,
	}

	assert.Equal(t, int64(15000), config.TimeToRetry.Milliseconds())
	assert.Equal(t, int64(60000), config.TimeToClaim.Milliseconds())
}

func TestAddrParsing(t *testing.T) {
	tests := []struct {
		name       string
		json       string
		want_topic string
		want_err   bool
	}{
		{
			name:       "SimpleTopic",
			json:       `{"topic":"my-topic"}`,
			want_topic: "my-topic",
			want_err:   false,
		},
		{
			name:       "TopicWithHyphens",
			json:       `{"topic":"my-topic-name"}`,
			want_topic: "my-topic-name",
			want_err:   false,
		},
		{
			name:       "FullTopicPath",
			json:       `{"topic":"projects/my-project/topics/my-topic"}`,
			want_topic: "projects/my-project/topics/my-topic",
			want_err:   false,
		},
		{
			name:     "EmptyTopic",
			json:     `{"topic":""}`,
			want_err: false,
		},
		{
			name:     "MissingTopic",
			json:     `{}`,
			want_err: false,
		},
		{
			name:     "InvalidJSON",
			json:     `{invalid}`,
			want_err: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var addr Addr
			err := json.Unmarshal([]byte(tt.json), &addr)

			if tt.want_err {
				assert.Error(t, err)
			} else {
				assert.Nil(t, err)
				if tt.want_topic != "" {
					assert.Equal(t, tt.want_topic, addr.Topic)
				}
			}
		})
	}
}

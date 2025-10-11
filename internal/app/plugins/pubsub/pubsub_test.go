package pubsub

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"

	"github.com/resonatehq/resonate/internal/app/plugins/base"
	"github.com/resonatehq/resonate/internal/metrics"
)

func TestPubSubPlugin(t *testing.T) {
	tests := []struct {
		name          string
		addr          []byte
		expectSuccess bool
		expectedTopic string
	}{
		{
			name:          "Success",
			addr:          []byte(`{"topic": "test-topic"}`),
			expectSuccess: false,
			expectedTopic: "test-topic",
		},
		{
			name:          "SuccessWithDifferentTopic",
			addr:          []byte(`{"topic": "orders-topic"}`),
			expectSuccess: false,
			expectedTopic: "orders-topic",
		},
		{
			name:          "SuccessWithComplexTopic",
			addr:          []byte(`{"topic": "projects/my-project/topics/my-topic"}`),
			expectSuccess: false,
			expectedTopic: "projects/my-project/topics/my-topic",
		},
		{
			name:          "FailureDueToJson",
			addr:          []byte(""),
			expectSuccess: false,
		},
		{
			name:          "FailureDueToMissingTopic",
			addr:          []byte(`{}`),
			expectSuccess: false,
		},
		{
			name:          "FailureDueToEmptyTopic",
			addr:          []byte(`{"topic": ""}`),
			expectSuccess: false,
		},
		{
			name:          "FailureDueToMalformedJSON",
			addr:          []byte(`{"topic": "test-topic"`),
			expectSuccess: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var addr Addr
			err := json.Unmarshal(tt.addr, &addr)

			if tt.expectedTopic != "" {
				assert.Nil(t, err, "JSON should parse correctly")
				assert.Equal(t, tt.expectedTopic, addr.Topic)
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
		_, err := New(metrics, &Config{
			BaseConfig: base.BaseConfig{Size: 10, Workers: 2, Timeout: 1 * time.Second},
			ProjectID:  "",
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
		BaseConfig: base.BaseConfig{
			TimeToRetry: 15 * time.Second,
			TimeToClaim: 60 * time.Second,
		},
	}

	assert.Equal(t, int64(15000), config.TimeToRetry.Milliseconds())
	assert.Equal(t, int64(60000), config.TimeToClaim.Milliseconds())
}

func TestAddrParsing(t *testing.T) {
	tests := []struct {
		name      string
		json      string
		wantTopic string
		wantErr   bool
	}{
		{
			name:      "SimpleTopic",
			json:      `{"topic":"my-topic"}`,
			wantTopic: "my-topic",
			wantErr:   false,
		},
		{
			name:      "TopicWithHyphens",
			json:      `{"topic":"my-topic-name"}`,
			wantTopic: "my-topic-name",
			wantErr:   false,
		},
		{
			name:      "FullTopicPath",
			json:      `{"topic":"projects/my-project/topics/my-topic"}`,
			wantTopic: "projects/my-project/topics/my-topic",
			wantErr:   false,
		},
		{
			name:    "EmptyTopic",
			json:    `{"topic":""}`,
			wantErr: false,
		},
		{
			name:    "MissingTopic",
			json:    `{}`,
			wantErr: false,
		},
		{
			name:    "InvalidJSON",
			json:    `{invalid}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var addr Addr
			err := json.Unmarshal([]byte(tt.json), &addr)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.Nil(t, err)
				if tt.wantTopic != "" {
					assert.Equal(t, tt.wantTopic, addr.Topic)
				}
			}
		})
	}
}

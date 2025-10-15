package redpanda

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/metrics"
)

type produceRequest struct {
	Records []struct {
		Key     string `json:"key,omitempty"`
		Value   string `json:"value"`
		Headers []struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		} `json:"headers,omitempty"`
	} `json:"records"`
}

func TestRedpandaPlugin(t *testing.T) {
	metrics := metrics.New(prometheus.NewRegistry())

	successServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "application/vnd.kafka.binary.v2+json", r.Header.Get("Content-Type"))

		var req produceRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		assert.NoError(t, err)

		assert.Len(t, req.Records, 1)
		record := req.Records[0]
		assert.Equal(t, base64.StdEncoding.EncodeToString([]byte("payload")), record.Value)
		if strings.Contains(r.URL.Path, "override") {
			assert.Equal(t, base64.StdEncoding.EncodeToString([]byte("key")), record.Key)
			assert.Len(t, record.Headers, 1)
			assert.Equal(t, "h1", record.Headers[0].Key)
			assert.Equal(t, base64.StdEncoding.EncodeToString([]byte("v1")), record.Headers[0].Value)
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer successServer.Close()

	failureServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer failureServer.Close()

	for _, tc := range []struct {
		name     string
		addr     []byte
		config   *Config
		expected bool
	}{
		{
			name:     "SuccessDefault",
			addr:     []byte(`{}`),
			config:   &Config{Size: 1, Workers: 1, Endpoints: []string{successServer.URL}, Topic: "default"},
			expected: true,
		},
		{
			name:     "SuccessOverride",
			addr:     []byte(`{"endpoint":"` + successServer.URL + `","topic":"override","key":"key","headers":{"h1":"v1"}}`),
			config:   &Config{Size: 1, Workers: 1, Endpoints: []string{failureServer.URL}, Topic: "default"},
			expected: true,
		},
		{
			name:     "FailureJSON",
			addr:     []byte(`{"endpoint":"` + successServer.URL + `","topic":`),
			config:   &Config{Size: 1, Workers: 1, Endpoints: []string{successServer.URL}, Topic: "default"},
			expected: false,
		},
		{
			name:     "FailureHTTP",
			addr:     []byte(`{"endpoint":"` + failureServer.URL + `","topic":"default"}`),
			config:   &Config{Size: 1, Workers: 1, Endpoints: []string{successServer.URL}, Topic: "default"},
			expected: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plugin, err := newWithFactory(nil, metrics, tc.config, func(timeout time.Duration) *http.Client {
				client := &http.Client{Timeout: timeout}
				return client
			})
			assert.NoError(t, err)

			assert.NoError(t, plugin.Start(nil))

			done := make(chan *t_aio.SenderCompletion, 1)
			ok := plugin.Enqueue(&aio.Message{
				Addr: tc.addr,
				Body: []byte("payload"),
				Done: func(completion *t_aio.SenderCompletion) {
					done <- completion
				},
			})
			assert.True(t, ok)

			completion := <-done
			assert.Equal(t, tc.expected, completion.Success)

			assert.NoError(t, plugin.Stop())
		})
	}
}

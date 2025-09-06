package sqs

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/metrics"
)

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
			sqsPlugin, err := New(nil, metrics, &Config{Size: 1, Workers: 1, Timeout: 1 * time.Second})
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

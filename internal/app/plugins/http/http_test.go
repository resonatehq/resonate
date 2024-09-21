package http

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/stretchr/testify/assert"
)

func TestHttpPlugin(t *testing.T) {
	// create a backchannel
	ch := make(chan []byte, 1)

	// start a mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}

		// send the request to the backchannel
		ch <- b

		switch r.URL.Path {
		case "/ok":
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))

	defer server.Close()

	metrics := metrics.New(prometheus.NewRegistry())

	okUrl := fmt.Sprintf("%s/ok", server.URL)
	koUrl := fmt.Sprintf("%s/ko", server.URL)

	for _, tc := range []struct {
		name    string
		data    *Data
		success bool
	}{
		{"ok", &Data{Url: okUrl}, true},
		{"ko", &Data{Url: koUrl}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			http, err := New(nil, metrics, &Config{Size: 1, Workers: 1, Timeout: 1 * time.Second})
			assert.Nil(t, err)

			data, err := json.Marshal(tc.data)
			assert.Nil(t, err)

			err = http.Start(nil)
			assert.Nil(t, err)

			ok := http.Enqueue(&aio.Message{
				Data: data,
				Body: []byte("ok"),
				Done: func(ok bool, err error) {
					assert.Nil(t, err)
					assert.Equal(t, tc.success, ok)
				},
			})

			assert.True(t, ok)
			assert.Equal(t, []byte("ok"), <-ch)

			err = http.Stop()
			assert.Nil(t, err)
		})
	}
}

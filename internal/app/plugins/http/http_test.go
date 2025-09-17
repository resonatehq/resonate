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
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/stretchr/testify/assert"
)

func TestHttpPlugin(t *testing.T) {
	// create a backchannel
	ch := make(chan []byte, 1)
	defer close(ch)

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
		data    *Addr
		success bool
	}{
		{"ok", &Addr{Url: okUrl}, true},
		{"ko", &Addr{Url: koUrl}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			http, err := New(nil, metrics, &Config{Size: 1, Workers: 1, Timeout: 1 * time.Second})
			assert.Nil(t, err)

			data, err := json.Marshal(tc.data)
			assert.Nil(t, err)

			err = http.Start(nil)
			assert.Nil(t, err)

			ok := http.Enqueue(&aio.Message{
				Addr: data,
				Body: []byte("ok"),
				Done: func(completion *t_aio.SenderCompletion) {
					assert.Equal(t, tc.success, completion.Success)
				},
			})

			assert.True(t, ok)
			assert.Equal(t, []byte("ok"), <-ch)

			err = http.Stop()
			assert.Nil(t, err)
		})
	}
}

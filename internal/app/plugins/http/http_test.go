package http

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
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

type request struct {
	head map[string][]string
	body []byte
}

func TestHttpPlugin(t *testing.T) {
	// create a backchannel
	ch := make(chan *request, 1)
	defer close(ch)

	// start a mock server that handles /ok and /ko
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		ch <- &request{r.Header, body}

		switch r.URL.Path {
		case "/ok":
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	metrics := metrics.New(prometheus.NewRegistry())

	// Normal working endpoints
	okUrl := fmt.Sprintf("%s/ok", server.URL)
	koUrl := fmt.Sprintf("%s/ko", server.URL)

	// ---- Simulate unreachable server ----
	l, err := net.Listen("tcp", "127.0.0.1:0")
	assert.Nil(t, err)
	unreachableAddr := l.Addr().String()
	_ = l.Close() // Now nothing is listening on that port
	unreachableUrl := fmt.Sprintf("http://%s/ko", unreachableAddr)
	// -------------------------------------

	for _, tc := range []struct {
		name string
		data *Addr
	}{
		{"ok", &Addr{Url: okUrl}},
		{"ko", &Addr{Url: koUrl}},
		{"unreachable", &Addr{Url: unreachableUrl}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			http, err := New(nil, metrics, &Config{Size: 1, Workers: 1, Timeout: 1 * time.Second, TimeToRetry: 15 * time.Second, TimeToClaim: 1 * time.Minute})
			assert.Nil(t, err)

			data, err := json.Marshal(tc.data)
			assert.Nil(t, err)

			err = http.Start(nil)
			assert.Nil(t, err)

			ok := http.Enqueue(&aio.Message{
				Addr: data,
				Head: map[string]string{"foo": "bar", "baz": "qux"},
				Body: []byte("ok"),
				Done: func(completion *t_aio.SenderCompletion) {
					assert.NotNil(t, completion)
				},
			})
			assert.True(t, ok)

			err = http.Stop()
			assert.Nil(t, err)
		})
	}
}

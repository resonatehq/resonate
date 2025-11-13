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

type request struct {
	head map[string][]string
	body []byte
}

func TestHttpPlugin(t *testing.T) {
	// create a backchannel
	ch := make(chan *request, 1)
	defer close(ch)

	// start a mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}

		// send the request to the backchannel
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

	okUrl := fmt.Sprintf("%s/ok", server.URL)
	koUrl := fmt.Sprintf("%s/ko", server.URL)
	toUrl := fmt.Sprintf("%s/to", server.URL)

	for _, tc := range []struct {
		name    string
		data    *Addr
		success bool
	}{
		{"ok", &Addr{Url: okUrl}, true},
		{"ko", &Addr{Url: koUrl}, true},
		{"to", &Addr{Url: toUrl}, true},
		{"connTo", &Addr{Url: "http://localhost:32412124"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			http, err := New(nil, metrics, &Config{Size: 1, Workers: 1, Timeout: 1 * time.Second, TimeToRetry: 15 * time.Second, TimeToClaim: 1 * time.Minute})
			assert.Nil(t, err)

			data, err := json.Marshal(tc.data)
			assert.Nil(t, err)

			err = http.Start(nil)
			assert.Nil(t, err)

			msg := &aio.Message{
				Addr: data,
				Head: map[string]string{"foo": "bar", "baz": "qux"},
				Body: []byte("ok"),
				Done: func(completion *t_aio.SenderCompletion) {
					assert.Equal(t, completion.Success, tc.success)
					assert.NotNil(t, completion)
				},
			}
			assert.True(t, http.Enqueue(msg))

			switch tc.name {
			case "ok", "ko", "to":
				req := <-ch
				assert.Equal(t, []string{"bar"}, req.head["Foo"])
				assert.Equal(t, []string{"qux"}, req.head["Baz"])
				assert.Equal(t, []byte("ok"), req.body)
			}

			err = http.Stop()
			assert.Nil(t, err)
		})
	}
}

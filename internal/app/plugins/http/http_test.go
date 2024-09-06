package http

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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
			http, err := New(&Config{Timeout: 1 * time.Second})
			assert.Nil(t, err)

			b, err := json.Marshal(tc.data)
			assert.Nil(t, err)

			// queue
			ok, err := http.Enqueue(b, []byte("ok"))
			assert.Nil(t, err)
			assert.Equal(t, tc.success, ok)
			assert.Equal(t, []byte("ok"), <-ch)
		})
	}
}

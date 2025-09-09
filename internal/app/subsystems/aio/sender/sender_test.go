package sender

import (
	"testing"

	"github.com/resonatehq/resonate/pkg/receiver"
	"github.com/stretchr/testify/assert"
)

func TestUrlParse(t *testing.T) {
	tests := []struct {
		name string
		url  string
		recv *receiver.Recv
		ok   bool
	}{
		// http/https scheme
		{
			name: "valid http url",
			url:  "http://example.com",
			recv: &receiver.Recv{Type: "http", Data: []byte(`{"url":"http://example.com"}`)},
			ok:   true,
		},
		{
			name: "valid https url",
			url:  "https://example.com",
			recv: &receiver.Recv{Type: "http", Data: []byte(`{"url":"https://example.com"}`)},
			ok:   true,
		},
		// poll scheme
		{
			name: "valid uni poll url",
			url:  "poll://uni@default",
			recv: &receiver.Recv{Type: "poll", Data: []byte(`{"cast":"uni","group":"default"}`)},
			ok:   true,
		},
		{
			name: "valid any poll url",
			url:  "poll://any@default",
			recv: &receiver.Recv{Type: "poll", Data: []byte(`{"cast":"any","group":"default"}`)},
			ok:   true,
		},
		{
			name: "valid uni poll url with id",
			url:  "poll://uni@default/abc",
			recv: &receiver.Recv{Type: "poll", Data: []byte(`{"cast":"uni","group":"default","id":"abc"}`)},
			ok:   true,
		},
		{
			name: "valid any poll url with id",
			url:  "poll://any@default/xyz",
			recv: &receiver.Recv{Type: "poll", Data: []byte(`{"cast":"any","group":"default","id":"xyz"}`)},
			ok:   true,
		},
		{
			name: "cast defaults to any if not provided",
			url:  "poll://default",
			recv: &receiver.Recv{Type: "poll", Data: []byte(`{"cast":"any","group":"default"}`)},
			ok:   true,
		},
		{
			name: "cast defaults to any if not provided (with id)",
			url:  "poll://default/xyz",
			recv: &receiver.Recv{Type: "poll", Data: []byte(`{"cast":"any","group":"default","id":"xyz"}`)},
			ok:   true,
		},
		{
			name: "cast must be one of uni or any",
			url:  "poll://nope@default",
			recv: nil,
			ok:   false,
		},
		// unsupported scheme
		{
			name: "scheme must be http, https, or poll",
			url:  "nope://example.com",
			recv: nil,
			ok:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recv, ok := schemeToRecv(tt.url)
			assert.Equal(t, tt.recv, recv)
			assert.Equal(t, tt.ok, ok)
		})
	}
}

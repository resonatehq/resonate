package poll

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/resonatehq/resonate/internal/plugins"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/stretchr/testify/assert"
)

type Conn struct {
	group string
	id    string
}

type Mesg struct {
	ok   bool
	mesg *plugins.Message
}

type Resp struct {
	group string
	id    []string
	data  string
}

func TestPollPlugin(t *testing.T) {
	metrics := metrics.New(prometheus.NewRegistry())

	for _, tc := range []struct {
		name        string
		mc          int
		connections []*Conn
		messages    []*Mesg
		expected    []*Resp
	}{
		{
			name: "SameGroup",
			mc:   5,
			connections: []*Conn{
				{"foo", "a"},
				{"foo", "b"},
			},
			messages: []*Mesg{
				{true, &plugins.Message{Addr: []byte(`{"cast":"any","group":"foo","id":"c"}`), Body: []byte("ok1")}},
				{true, &plugins.Message{Addr: []byte(`{"cast":"any","group":"foo","id":"c"}`), Body: []byte("ok2")}},
				{true, &plugins.Message{Addr: []byte(`{"cast":"any","group":"foo","id":"c"}`), Body: []byte("ok3")}},
			},
			expected: []*Resp{
				{"foo", []string{"a", "b"}, "data: ok1"},
				{"foo", []string{"a", "b"}, "data: ok2"},
				{"foo", []string{"a", "b"}, "data: ok3"},
			},
		},
		{
			name: "SameGroupWithHeaders",
			mc:   5,
			connections: []*Conn{
				{"foo", "a"},
				{"foo", "b"},
			},
			messages: []*Mesg{
				{true, &plugins.Message{Addr: []byte(`{"cast":"any","group":"foo","id":"c"}`), Head: map[string]string{"foo": "bar"}, Body: []byte(`{"body": "ok1"}`)}},
				{true, &plugins.Message{Addr: []byte(`{"cast":"any","group":"foo","id":"c"}`), Head: map[string]string{"baz": "qux"}, Body: []byte(`{"body": "ok2"}`)}},
				{true, &plugins.Message{Addr: []byte(`{"cast":"any","group":"foo","id":"c"}`), Head: map[string]string{"foo": "bar", "baz": "qux"}, Body: []byte(`{"body": "ok3"}`)}},
			},
			expected: []*Resp{
				{"foo", []string{"a", "b"}, `data: {"body":"ok1","head":{"foo":"bar"}}`},
				{"foo", []string{"a", "b"}, `data: {"body":"ok2","head":{"baz":"qux"}}`},
				{"foo", []string{"a", "b"}, `data: {"body":"ok3","head":{"baz":"qux","foo":"bar"}}`},
			},
		},
		{
			name: "SameGroupAndId",
			mc:   5,
			connections: []*Conn{
				{"foo", "a"},
				{"foo", "b"},
			},
			messages: []*Mesg{
				{true, &plugins.Message{Addr: []byte(`{"cast":"any","group":"foo","id":"a"}`), Body: []byte("ok1")}},
				{true, &plugins.Message{Addr: []byte(`{"cast":"any","group":"foo","id":"a"}`), Body: []byte("ok2")}},
				{true, &plugins.Message{Addr: []byte(`{"cast":"any","group":"foo","id":"a"}`), Body: []byte("ok3")}},
			},
			expected: []*Resp{
				{"foo", []string{"a"}, "data: ok1"},
				{"foo", []string{"a"}, "data: ok2"},
				{"foo", []string{"a"}, "data: ok3"},
			},
		},
		{
			name: "SameGroupNoId",
			mc:   5,
			connections: []*Conn{
				{"foo", "a"},
				{"foo", "b"},
			},
			messages: []*Mesg{
				{true, &plugins.Message{Addr: []byte(`{"cast":"any","group":"foo"}`), Body: []byte("ok1")}},
				{true, &plugins.Message{Addr: []byte(`{"cast":"any","group":"foo"}`), Body: []byte("ok2")}},
				{true, &plugins.Message{Addr: []byte(`{"cast":"any","group":"foo"}`), Body: []byte("ok3")}},
			},
			expected: []*Resp{
				{"foo", []string{"a", "b"}, "data: ok1"},
				{"foo", []string{"a", "b"}, "data: ok2"},
				{"foo", []string{"a", "b"}, "data: ok3"},
			},
		},
		{
			name: "SameGroupAndIdWithSlashes",
			mc:   5,
			connections: []*Conn{
				{"foo", "a/b"},
				{"foo", "c/d"},
			},
			messages: []*Mesg{
				{true, &plugins.Message{Addr: []byte(`{"cast":"any","group":"foo","id":"a/b"}`), Body: []byte("ok1")}},
				{true, &plugins.Message{Addr: []byte(`{"cast":"any","group":"foo","id":"c/d"}`), Body: []byte("ok2")}},
				{true, &plugins.Message{Addr: []byte(`{"cast":"any","group":"foo","id":"e/f"}`), Body: []byte("ok3")}},
			},
			expected: []*Resp{
				{"foo", []string{"a/b"}, "data: ok1"},
				{"foo", []string{"c/d"}, "data: ok2"},
				{"foo", []string{"a/b", "c/d"}, "data: ok3"},
			},
		},
		{
			name: "MultipleGroups",
			mc:   5,
			connections: []*Conn{
				{"foo", "a"},
				{"foo", "b"},
				{"bar", "a"},
				{"bar", "b"},
			},
			messages: []*Mesg{
				{true, &plugins.Message{Addr: []byte(`{"cast":"any","group":"foo","id":"a"}`), Body: []byte("ok1")}},
				{true, &plugins.Message{Addr: []byte(`{"cast":"any","group":"foo","id":"b"}`), Body: []byte("ok2")}},
				{true, &plugins.Message{Addr: []byte(`{"cast":"any","group":"foo","id":"c"}`), Body: []byte("ok3")}},
				{true, &plugins.Message{Addr: []byte(`{"cast":"any","group":"bar","id":"a"}`), Body: []byte("ok1")}},
				{true, &plugins.Message{Addr: []byte(`{"cast":"any","group":"bar","id":"b"}`), Body: []byte("ok2")}},
				{true, &plugins.Message{Addr: []byte(`{"cast":"any","group":"bar","id":"c"}`), Body: []byte("ok3")}},
				{false, &plugins.Message{Addr: []byte(`{"cast":"any","group":"baz","id":"a"}`), Body: []byte("ko1")}},
				{false, &plugins.Message{Addr: []byte(`{"cast":"any","group":"baz","id":"b"}`), Body: []byte("ko2")}},
				{false, &plugins.Message{Addr: []byte(`{"cast":"any","group":"baz","id":"c"}`), Body: []byte("ko3")}},
			},
			expected: []*Resp{
				{"foo", []string{"a"}, "data: ok1"},
				{"foo", []string{"b"}, "data: ok2"},
				{"foo", []string{"a", "b"}, "data: ok3"},
				{"bar", []string{"a"}, "data: ok1"},
				{"bar", []string{"b"}, "data: ok2"},
				{"bar", []string{"a", "b"}, "data: ok3"},
			},
		},
		{
			name: "NoConnection",
			mc:   5,
			messages: []*Mesg{
				{false, &plugins.Message{Addr: []byte(`{"cast":"any","group":"foo","id":"a"}`), Body: []byte("ko1")}},
				{false, &plugins.Message{Addr: []byte(`{"cast":"any","group":"foo","id":"b"}`), Body: []byte("ko2")}},
				{false, &plugins.Message{Addr: []byte(`{"cast":"any","group":"foo","id":"c"}`), Body: []byte("ko3")}},
			},
		},
		{
			name: "ConnectionReplacesConnection",
			mc:   5,
			connections: []*Conn{
				{"foo", "a"},
				{"foo", "a"},
			},
			messages: []*Mesg{
				{true, &plugins.Message{Addr: []byte(`{"cast":"any","group":"foo","id":"a"}`), Body: []byte("ok1")}},
				{true, &plugins.Message{Addr: []byte(`{"cast":"any","group":"foo","id":"b"}`), Body: []byte("ok2")}},
				{true, &plugins.Message{Addr: []byte(`{"cast":"any","group":"foo","id":"c"}`), Body: []byte("ok3")}},
			},
			expected: []*Resp{
				{"foo", []string{"a"}, "data: ok1"},
				{"foo", []string{"a"}, "data: ok2"},
				{"foo", []string{"a"}, "data: ok3"},
			},
		},
		{
			name: "CannotExceedMaxConnections",
			mc:   3,
			connections: []*Conn{
				{"foo", "a"},
				{"foo", "b"},
				{"foo", "c"},
				{"foo", "d"},
			},
			messages: []*Mesg{
				{true, &plugins.Message{Addr: []byte(`{"cast":"any","group":"foo","id":"d"}`), Body: []byte("ok1")}},
				{true, &plugins.Message{Addr: []byte(`{"cast":"any","group":"foo","id":"d"}`), Body: []byte("ok2")}},
				{true, &plugins.Message{Addr: []byte(`{"cast":"any","group":"foo","id":"d"}`), Body: []byte("ok3")}},
			},
			expected: []*Resp{
				{"foo", []string{"a", "b", "c"}, "data: ok1"},
				{"foo", []string{"a", "b", "c"}, "data: ok2"},
				{"foo", []string{"a", "b", "c"}, "data: ok3"},
			},
		},
		{
			name: "UnicastMustBeSameGroupAndId",
			mc:   5,
			connections: []*Conn{
				{"foo", "a"},
			},
			messages: []*Mesg{
				{true, &plugins.Message{Addr: []byte(`{"cast":"uni","group":"foo","id":"a"}`), Body: []byte("ok1")}},
				{false, &plugins.Message{Addr: []byte(`{"cast":"uni","group":"foo","id":"b"}`), Body: []byte("ok2")}},
				{false, &plugins.Message{Addr: []byte(`{"cast":"uni","group":"foo","id":"c"}`), Body: []byte("ok3")}},
			},
			expected: []*Resp{
				{"foo", []string{"a"}, "data: ok1"},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := &Config{
				Size:           100,
				BufferSize:     100,
				MaxConnections: tc.mc,
				Addr:           ":0",
				Timeout:        1 * time.Second,
			}

			poll, err := New(metrics, config)
			assert.Nil(t, err)

			errors := make(chan error, 100)
			if err := poll.Start(errors); err != nil {
				t.Fatal(err)
			}

			backchannel := make(chan struct{ group, id, data string }, len(tc.messages))
			var disconnected sync.WaitGroup

			// establish connections
			for _, conn := range tc.connections {
				res, err := http.Get(fmt.Sprintf("http://%s/%s/%s", poll.Addr(), conn.group, conn.id))
				if err != nil {
					t.Fatal(err)
				}

				if res.StatusCode != http.StatusOK {
					continue
				}

				disconnected.Add(1)

				reader := bufio.NewReader(res.Body)
				defer util.DeferAndLog(res.Body.Close)

				go func() {
					defer disconnected.Done()

					for {
						// read each SSE message
						data, err := reader.ReadBytes('\n')
						if err == io.EOF {
							return
						} else if err != nil {
							// panic because we are in a goroutine
							panic(err)
						}

						// SSE messages are terminated by two newlines but ReadBytes
						// requires a single byte delimiter, so skip the second read
						if string(data) == "\n" {
							continue
						}

						// send the message to the backchannel
						backchannel <- struct{ group, id, data string }{conn.group, conn.id, strings.TrimSpace(string(data))}
					}
				}()
			}

			// send messages
			for _, mesg := range tc.messages {
				ch := make(chan any)

				mesg.mesg.Done = func(completion *t_aio.SenderCompletion) {
					defer close(ch)
					assert.Equal(t, mesg.ok, completion.Success)
				}
				assert.True(t, poll.Enqueue(mesg.mesg))
				<-ch
			}

			// count and assert active connections
			ac := 0
			for _, c := range poll.worker.connections.conns {
				ac = ac + len(c)
			}
			assert.Equal(t, ac, poll.worker.connections.len)

			// assert max connections is not exceeded
			assert.LessOrEqual(t, ac, config.MaxConnections)

			// count unique connections
			uc := unique(tc.connections)

			// assert ac is equal to min of uc/mc
			assert.Equal(t, min(uc, config.MaxConnections), ac)

			// close the poll server
			if err := poll.Stop(); err != nil {
				t.Fatal(err)
			}

			// wait until all connections have disconnected
			disconnected.Wait()

			// close the channels
			close(errors)
			close(backchannel)

			// assert no active connections
			assert.Zero(t, poll.worker.connections.len)

			// assert no errors
			for err := range errors {
				assert.Fail(t, err.Error())
			}

			// assert backchannel messages
			for mesg := range backchannel {
				assert.Condition(t, func() bool {
					for i, expected := range tc.expected {
						if mesg.group == expected.group && slices.Contains(expected.id, mesg.id) && mesg.data == expected.data {
							// remove from expected
							tc.expected = append(tc.expected[:i], tc.expected[i+1:]...)
							return true
						}
					}
					return false
				})
			}

			// all expected messages should have been received
			assert.Empty(t, tc.expected, "expected messages not received")
		})
	}
}

func TestPollPluginAuth(t *testing.T) {
	metrics := metrics.New(prometheus.NewRegistry())

	config := &Config{
		Size:           10,
		BufferSize:     10,
		MaxConnections: 1,
		Addr:           ":0",
		Timeout:        1 * time.Second,
		Auth:           map[string]string{"user": "pass"},
	}

	poll, err := New(metrics, config)
	assert.Nil(t, err)

	errors := make(chan error, 10)
	assert.Nil(t, poll.Start(errors))

	client := &http.Client{Timeout: 1 * time.Second}

	tests := []struct {
		name     string
		username string
		password string
		code     int
	}{
		{"CorrectCredentials", "user", "pass", http.StatusOK},
		{"IncorrectCredentials", "user", "wrong", http.StatusUnauthorized},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest("GET", fmt.Sprintf("http://%s/foo/a", poll.Addr()), nil)
			if err != nil {
				t.Fatal(err)
			}

			req.SetBasicAuth(tc.username, tc.password)

			res, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer util.DeferAndLog(res.Body.Close)

			assert.Equal(t, tc.code, res.StatusCode)
		})
	}

	if err := poll.Stop(); err != nil {
		t.Fatal(err)
	}

	close(errors)
	for err := range errors {
		assert.Fail(t, err.Error())
	}
}

func unique(conns []*Conn) int {
	set := make(map[string]struct{})
	for _, c := range conns {
		set[fmt.Sprintf("%s.%s", c.group, c.id)] = struct{}{}
	}

	return len(set)
}

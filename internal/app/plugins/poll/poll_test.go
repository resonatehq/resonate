package poll

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/stretchr/testify/assert"
)

type Conn struct {
	group string
	id    string
}

type Mesg struct {
	ok   bool
	mesg *aio.Message
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
			name: "SameGroupAndId",
			mc:   5,
			connections: []*Conn{
				{"foo", "a"},
				{"foo", "b"},
			},
			messages: []*Mesg{
				{true, &aio.Message{Data: []byte(`{"group":"foo","id":"a"}`), Body: []byte("ok1")}},
				{true, &aio.Message{Data: []byte(`{"group":"foo","id":"a"}`), Body: []byte("ok2")}},
				{true, &aio.Message{Data: []byte(`{"group":"foo","id":"a"}`), Body: []byte("ok3")}},
			},
			expected: []*Resp{
				{"foo", []string{"a"}, "data: ok1"},
				{"foo", []string{"a"}, "data: ok2"},
				{"foo", []string{"a"}, "data: ok3"},
			},
		},
		{
			name: "SameGroup",
			mc:   5,
			connections: []*Conn{
				{"foo", "a"},
				{"foo", "b"},
			},
			messages: []*Mesg{
				{true, &aio.Message{Data: []byte(`{"group":"foo","id":"c"}`), Body: []byte("ok1")}},
				{true, &aio.Message{Data: []byte(`{"group":"foo","id":"c"}`), Body: []byte("ok2")}},
				{true, &aio.Message{Data: []byte(`{"group":"foo","id":"c"}`), Body: []byte("ok3")}},
			},
			expected: []*Resp{
				{"foo", []string{"a", "b"}, "data: ok1"},
				{"foo", []string{"a", "b"}, "data: ok2"},
				{"foo", []string{"a", "b"}, "data: ok3"},
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
				{true, &aio.Message{Data: []byte(`{"group":"foo","id":"a"}`), Body: []byte("ok1")}},
				{true, &aio.Message{Data: []byte(`{"group":"foo","id":"b"}`), Body: []byte("ok2")}},
				{true, &aio.Message{Data: []byte(`{"group":"foo","id":"c"}`), Body: []byte("ok3")}},
				{true, &aio.Message{Data: []byte(`{"group":"bar","id":"a"}`), Body: []byte("ok1")}},
				{true, &aio.Message{Data: []byte(`{"group":"bar","id":"b"}`), Body: []byte("ok2")}},
				{true, &aio.Message{Data: []byte(`{"group":"bar","id":"c"}`), Body: []byte("ok3")}},
				{false, &aio.Message{Data: []byte(`{"group":"baz","id":"a"}`), Body: []byte("ko1")}},
				{false, &aio.Message{Data: []byte(`{"group":"baz","id":"b"}`), Body: []byte("ko2")}},
				{false, &aio.Message{Data: []byte(`{"group":"baz","id":"c"}`), Body: []byte("ko3")}},
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
				{false, &aio.Message{Data: []byte(`{"group":"foo","id":"a"}`), Body: []byte("ko1")}},
				{false, &aio.Message{Data: []byte(`{"group":"foo","id":"b"}`), Body: []byte("ko2")}},
				{false, &aio.Message{Data: []byte(`{"group":"foo","id":"c"}`), Body: []byte("ko3")}},
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
				{true, &aio.Message{Data: []byte(`{"group":"foo","id":"a"}`), Body: []byte("ok1")}},
				{true, &aio.Message{Data: []byte(`{"group":"foo","id":"b"}`), Body: []byte("ok2")}},
				{true, &aio.Message{Data: []byte(`{"group":"foo","id":"c"}`), Body: []byte("ok3")}},
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
				{true, &aio.Message{Data: []byte(`{"group":"foo","id":"d"}`), Body: []byte("ok1")}},
				{true, &aio.Message{Data: []byte(`{"group":"foo","id":"d"}`), Body: []byte("ok2")}},
				{true, &aio.Message{Data: []byte(`{"group":"foo","id":"d"}`), Body: []byte("ok3")}},
			},
			expected: []*Resp{
				{"foo", []string{"a", "b", "c"}, "data: ok1"},
				{"foo", []string{"a", "b", "c"}, "data: ok2"},
				{"foo", []string{"a", "b", "c"}, "data: ok3"},
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

			poll, err := New(nil, metrics, config)
			assert.Nil(t, err)

			errors := make(chan error, 100)
			if err := poll.Start(errors); err != nil {
				t.Fatal(err)
			}

			backchannel := make(chan struct{ group, id, data string }, len(tc.messages))
			var disconnected sync.WaitGroup

			// establish connections
			for _, conn := range tc.connections {
				disconnected.Add(1)

				res, err := http.Get(fmt.Sprintf("http://%s/%s/%s", poll.Addr(), conn.group, conn.id))
				if err != nil {
					t.Fatal(err)
				}

				go func() {
					defer res.Body.Close()
					defer disconnected.Done()

					reader := bufio.NewReader(res.Body)
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

			// count and assert active connections
			ac := 0
			for _, c := range poll.worker.connections.conns {
				ac = ac + len(c)
			}
			assert.Equal(t, poll.worker.connections.len, ac)

			// assert max connections is not exceeded
			assert.LessOrEqual(t, ac, config.MaxConnections)

			// count unique connections
			uc := unique(tc.connections)

			// assert ac is equal to min of uc/mc
			assert.Equal(t, ac, min(uc, config.MaxConnections))

			// send messages
			for _, mesg := range tc.messages {
				mesg.mesg.Done = func(ok bool, err error) {
					assert.Equal(t, mesg.ok, ok)
					if ok {
						assert.Nil(t, err)
					} else {
						assert.NotNil(t, err)
					}
				}
				assert.True(t, poll.Enqueue(mesg.mesg))
			}

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
						if mesg.group == expected.group && contains(expected.id, mesg.id) && mesg.data == expected.data {
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

func contains[T comparable](arr []T, val T) bool {
	for _, v := range arr {
		if v == val {
			return true
		}
	}
	return false
}

func unique(conns []*Conn) int {
	set := make(map[string]struct{})
	for _, c := range conns {
		set[fmt.Sprintf("%s.%s", c.group, c.id)] = struct{}{}
	}

	return len(set)
}

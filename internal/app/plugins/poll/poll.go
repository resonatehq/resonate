package poll

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand" // nosemgrep
	"net"
	"strings"
	"time"

	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/resonatehq/resonate/internal/util"
)

type Config struct {
	Size            int               `flag:"size" desc:"submission buffered channel size" default:"100"`
	BufferSize      int               `flag:"buffer-size" desc:"connection buffer size" default:"100"`
	MaxConnections  int               `flag:"max-connections" desc:"maximum number of connections" default:"1000"`
	Addr            string            `flag:"addr" desc:"http server address" default:":8002"`
	Cors            Cors              `flag:"cors" desc:"http cors settings"`
	Timeout         time.Duration     `flag:"timeout" desc:"http server graceful shutdown timeout" default:"10s"`
	DisconnectAfter time.Duration     `flag:"disconnect-after" desc:"time to wait before closing a connections, defaults to never" default:"0"`
	Auth            map[string]string `flag:"auth" desc:"http basic auth username password pairs"`
	TimeToRetry     time.Duration     `flag:"ttr" desc:"time to wait before resending" default:"15s"`
	TimeToClaim     time.Duration     `flag:"ttc" desc:"time to wait for claim before resending" default:"1m"`
}

type Cors struct {
	AllowOrigins []string `flag:"allow-origin" desc:"allowed origins, if not provided cors is not enabled"`
}

type Poll struct {
	sq         chan *aio.Message
	connect    chan *connection
	disconnect chan *connection
	worker     *PollWorker
	server     *PollServer
}

type Addr struct {
	Cast  string `json:"cast"`
	Group string `json:"group"`
	Id    string `json:"id,omitempty"`
}

func (a *Addr) String() string {
	id := ""
	if a.Id != "" {
		id = "/" + a.Id
	}

	return fmt.Sprintf("poll://%s@%s%s", a.Cast, a.Group, id)
}

type connection struct {
	group string
	id    string
	ch    chan []byte
}

type connections struct {
	max   int
	len   int
	cnt   prometheus.Gauge
	conns map[string][]*connection
}

func (cs *connections) get(addr *Addr) (*connection, bool) {
	util.Assert(addr.Cast == "uni" || addr.Cast == "any", "cast must be one of: uni, any")

	if len(cs.conns[addr.Group]) == 0 {
		return nil, false
	}

	// if id is provided, prefer connection with the same id
	if addr.Id != "" {
		for _, conn := range cs.conns[addr.Group] {
			if conn.id == addr.Id {
				return conn, true
			}
		}
	}

	// if type is uni, do not send address with different id
	if addr.Cast == "uni" {
		return nil, false
	}

	// if there is no connection with the same id, choose a
	// connection at random
	return cs.conns[addr.Group][rand.Intn(len(cs.conns[addr.Group]))], true
}

func (cs *connections) add(conn *connection) {
	util.Assert(conn.ch != nil, "channel must not be nil")

	// first remove the current connection
	cs.rmv(conn, false)

	// immediately close the connection if max connections is reached
	if cs.len >= cs.max {
		close(conn.ch)
		return
	}

	// then add the new connection
	cs.conns[conn.group] = append(cs.conns[conn.group], conn)
	cs.len++
	cs.cnt.Inc()
}

func (cs *connections) rmv(conn *connection, match bool) {
	util.Assert(conn.ch != nil, "channel must not be nil")

	// remove the connection iff the channels match, if the channels
	// don't match then the connection has been usurped (and already
	// closed)
	for i, c := range cs.conns[conn.group] {
		if c.id == conn.id && (c.ch == conn.ch || !match) {
			close(c.ch)
			cs.conns[conn.group] = append(cs.conns[conn.group][:i], cs.conns[conn.group][i+1:]...)
			cs.len--
			cs.cnt.Dec()
			return
		}
	}
}

func New(a aio.AIO, metrics *metrics.Metrics, config *Config) (*Poll, error) {
	sq := make(chan *aio.Message, config.Size)

	// connect channel is used to register new connections with the
	// connection manager (worker)
	connect := make(chan *connection, config.MaxConnections)

	// disconnect channel is used to unregister connections with the
	// connection manager (worker)
	disconnect := make(chan *connection, config.MaxConnections)

	handler := &PollHandler{
		config:     config,
		metrics:    metrics,
		connect:    connect,
		disconnect: disconnect,
	}

	listen, err := net.Listen("tcp", config.Addr)
	if err != nil {
		return nil, err
	}

	server := &PollServer{
		config: config,
		listen: listen,
		server: &http.Server{Handler: handler},
	}

	counter := metrics.AioConnection.WithLabelValues((&Poll{}).String())

	worker := &PollWorker{
		sq:         sq,
		config:     config,
		metrics:    metrics,
		counter:    counter,
		connect:    connect,
		disconnect: disconnect,
		connections: connections{
			max:   config.MaxConnections,
			cnt:   counter,
			conns: map[string][]*connection{},
		},
	}

	return &Poll{
		sq:         sq,
		connect:    connect,
		disconnect: disconnect,
		server:     server,
		worker:     worker,
	}, nil
}

func (p *Poll) String() string {
	return fmt.Sprintf("%s:poll", t_aio.Sender.String())
}

func (p *Poll) Type() string {
	return "poll"
}

func (p *Poll) Addr() string {
	return p.server.listen.Addr().String()
}

func (p *Poll) Start(errors chan<- error) error {
	go p.server.Start(errors)
	go p.worker.Start()

	return nil
}

func (p *Poll) Stop() error {
	// wait to close the connection channels, this can be done safely
	// once the server has been stopped
	defer close(p.connect)
	defer close(p.disconnect)

	// immediately close the sq, this will signal the worker to start
	// closing connections
	close(p.sq)

	// stop the server
	return p.server.Stop()
}

func (p *Poll) Enqueue(msg *aio.Message) bool {
	select {
	case p.sq <- msg:
		return true
	default:
		return false
	}
}

// Worker

type PollWorker struct {
	sq          <-chan *aio.Message
	config      *Config
	metrics     *metrics.Metrics
	counter     prometheus.Gauge
	connect     <-chan *connection
	disconnect  <-chan *connection
	connections connections
}

func (w *PollWorker) String() string {
	return fmt.Sprintf("%s:poll", t_aio.Sender.String())
}

func (w *PollWorker) Start() {
	counter := w.metrics.AioWorkerInFlight.WithLabelValues(w.String(), "0")
	w.metrics.AioWorker.WithLabelValues(w.String()).Inc()
	defer w.metrics.AioWorker.WithLabelValues(w.String()).Dec()

	var closed bool

	for {
		select {
		case conn, ok := <-w.connect:
			if !ok {
				return
			}

			// register a connection
			w.connections.add(conn)
		case conn, ok := <-w.disconnect:
			if !ok {
				return
			}

			// unregister a connection
			w.connections.rmv(conn, true)
		default:
			// Note: this select occurs under the default case in order to
			// prioritize the connect/disconnect channels, this minimizes the
			// chance of attempting to send a message before a connection has
			// been registered or after a connection has been closed

			if closed {
				break
			}

			select {
			case conn, ok := <-w.connect:
				if !ok {
					return
				}
				w.connections.add(conn)
			case conn, ok := <-w.disconnect:
				if !ok {
					return
				}
				w.connections.rmv(conn, true)
			case mesg, ok := <-w.sq:
				if !ok {
					closed = true
					break
				}

				counter.Inc()
				w.Process(mesg)
				counter.Dec()
			}
		}

		if closed {
			// when the server is closed don't immediately return because
			// additional connections may still be established, the
			// connect/disconnect channels will be closed once the server has
			// been stopped, return then
			for group, conns := range w.connections.conns {
				for _, conn := range conns {
					close(conn.ch)
					w.connections.len--
					w.counter.Dec()
				}
				delete(w.connections.conns, group)
			}
		}
	}
}

func (w *PollWorker) Process(mesg *aio.Message) {
	// unmarshal message
	var addr *Addr
	if err := json.Unmarshal(mesg.Addr, &addr); err != nil {
		slog.Warn("failed to parse address", "err", err)

		mesg.Done(&t_aio.SenderCompletion{
			Success:     false,
			TimeToRetry: w.config.TimeToRetry.Milliseconds(),
			TimeToClaim: w.config.TimeToClaim.Milliseconds(),
		})
		return
	}

	// check if we have a connection
	conn, ok := w.connections.get(addr)
	if !ok {
		slog.Warn("connection not found", "address", addr)

		mesg.Done(&t_aio.SenderCompletion{
			Success:     false,
			TimeToRetry: w.config.TimeToRetry.Milliseconds(),
			TimeToClaim: w.config.TimeToClaim.Milliseconds(),
		})
		return
	}

	// send message to connection
	select {
	case conn.ch <- mesg.Body:
		mesg.Done(&t_aio.SenderCompletion{
			Success:     true,
			TimeToRetry: w.config.TimeToRetry.Milliseconds(),
			TimeToClaim: w.config.TimeToClaim.Milliseconds(),
		})
	default:
		slog.Warn("connection group full", "address", addr)

		mesg.Done(&t_aio.SenderCompletion{
			Success:     false,
			TimeToRetry: w.config.TimeToRetry.Milliseconds(),
			TimeToClaim: w.config.TimeToClaim.Milliseconds(),
		})
	}
}

// Server

type PollServer struct {
	config *Config
	listen net.Listener
	server *http.Server
}

func (s *PollServer) Start(errors chan<- error) {
	slog.Info("starting poll server", "addr", s.config.Addr)
	if err := s.server.Serve(s.listen); err != nil && err != http.ErrServerClosed {
		errors <- err
	}
}

func (s *PollServer) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.Timeout)
	defer cancel()

	return s.server.Shutdown(ctx)
}

type PollHandler struct {
	config     *Config
	metrics    *metrics.Metrics
	connect    chan<- *connection
	disconnect chan<- *connection
}

func (h *PollHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// authentication
	if len(h.config.Auth) > 0 {
		username, password, ok := r.BasicAuth()
		if !ok {
			w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		if p, ok := h.config.Auth[username]; !ok || p != password {
			w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}

	// only GET requests are allowed
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// extract group and id
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 3 {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	// request must support flushing
	f, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	conn := &connection{
		group: parts[1],
		id:    strings.Join(parts[2:], "/"),
		ch:    make(chan []byte, h.config.BufferSize),
	}

	if !h.Connect(conn) {
		http.Error(w, "too many requests", http.StatusTooManyRequests)
		return
	}

	// now we can write headers and flush
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// cors headers
	if len(h.config.Cors.AllowOrigins) > 0 {
		if len(h.config.Cors.AllowOrigins) == 1 && h.config.Cors.AllowOrigins[0] == "*" {
			w.Header().Set("Access-Control-Allow-Origin", "*")
		} else {
			// according to the CORS spec the allow origin header must return only the
			// request origin header if it is in the allow list
			for _, origin := range h.config.Cors.AllowOrigins {
				if strings.EqualFold(origin, r.Header.Get("Origin")) {
					w.Header().Set("Access-Control-Allow-Origin", origin)
					break
				}
			}
		}
	}

	f.Flush()

	// some environments have a max connection time, this option lets
	// connections be closed gracefully, the sdk must reconnect
	var disconnect <-chan time.Time
	if h.config.DisconnectAfter > 0 {
		disconnect = time.After(h.config.DisconnectAfter)
	}

	for {
		select {
		case data, ok := <-conn.ch:
			if !ok {
				return
			}

			if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil { // nosemgrep: no-fprintf-to-responsewriter
				h.Disconnect(conn)
				return
			}

			f.Flush()
		case <-disconnect:
			h.Disconnect(conn)
			return
		case <-r.Context().Done():
			h.Disconnect(conn)
			return
		}
	}
}

func (h *PollHandler) Connect(conn *connection) bool {
	select {
	case h.connect <- conn:
		return true
	default:
		return false
	}
}

func (h *PollHandler) Disconnect(conn *connection) {
	select {
	case h.disconnect <- conn:
	default:
		panic("disconnect buffered channel must never be full")
	}
}

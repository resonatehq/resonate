package simulator

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"time"
)

// Server represent an already running implementation.
type System struct {
	Binary     string
	HttpURL    string
	GrpcURL    string
	MetricsURL string
	Logs       chan string
}

// StartServer starts a resonate server
func NewSystem(ctx context.Context) (*System, error) {
	httpPort, err := findUnusedTCPPort()
	if err != nil {
		return nil, err
	}

	grpcPort, err := findUnusedTCPPort()
	if err != nil {
		return nil, err
	}

	metricsPort, err := findUnusedTCPPort()
	if err != nil {
		return nil, err
	}

	// binary must be in path
	cmd, err := newCommand(
		"../../resonate",
		"serve",

		// TODO: separate pr for this
		fmt.Sprintf("--api-http-addr=0.0.0.0:%d", httpPort),
		fmt.Sprintf("--api-grpc-addr=0.0.0.0:%d", grpcPort),
		fmt.Sprintf("--metrics-port=%d", metricsPort),
	)
	if err != nil {
		return nil, err
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}

	logCh := make(chan string)
	go func() {
		defer close(logCh)
		defer stdout.Close()

		reader := bufio.NewScanner(stdout)
		for reader.Scan() {
			logCh <- reader.Text()
		}
	}()

	err = cmd.Start()
	if err != nil {
		return nil, err
	}

	srv := &System{
		Binary:     "../../resonate", // TODO: location of build, make this configurabele
		HttpURL:    fmt.Sprintf("http://localhost:%d/", httpPort),
		GrpcURL:    fmt.Sprintf("http://localhost:%d/", grpcPort),
		MetricsURL: fmt.Sprintf("http://localhost:%d/", metricsPort),
		Logs:       logCh,
	}

	return srv, nil
}

func (srv *System) IsReady() bool {
	serverAddr := strings.TrimSuffix(strings.TrimPrefix(srv.HttpURL, "http://"), "/")
	conn, err := net.DialTimeout("tcp", serverAddr, 1*time.Second)
	if err != nil {
		return false
	}
	defer conn.Close()
	return true
}

// Cleanup shuts down the server and deletes any on-disk files the server used.
func (srv *System) Cleanup() {}

func newCommand(name string, args ...string) (*exec.Cmd, error) {
	p, err := exec.LookPath(name)
	if err == nil {
		return exec.Command(p, args...), nil
	}
	return exec.Command(p, args...), nil
}

func findUnusedTCPPort() (int, error) {
	l, err := net.ListenTCP("tcp", &net.TCPAddr{
		IP: net.IPv4(127, 0, 0, 1),
	})
	if err != nil {
		return 0, err
	}

	port := l.Addr().(*net.TCPAddr).Port
	if err := l.Close(); err != nil {
		return 0, err
	}

	return port, nil
}

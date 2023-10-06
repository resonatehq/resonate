package t_aio

import (
	"fmt"
	"net/http"
)

type NetworkKind int

const (
	Http NetworkKind = iota
)

type NetworkSubmission struct {
	Kind NetworkKind
	Http *HttpRequest
}

func (s *NetworkSubmission) String() string {
	switch s.Kind {
	case Http:
		return fmt.Sprintf("Network(http=Http(method=%s, url=%s))", s.Http.Method, s.Http.Url)
	default:
		panic("invalid aio network submission")
	}
}

type NetworkCompletion struct {
	Kind NetworkKind
	Http *http.Response
}

func (c *NetworkCompletion) String() string {
	switch c.Kind {
	case Http:
		return fmt.Sprintf("Network(http=Http(status=%d))", c.Http.StatusCode)
	default:
		panic("invalid aio network completion")
	}
}

type HttpRequest struct {
	Headers map[string]string
	Method  string
	Url     string
	Body    []byte
}

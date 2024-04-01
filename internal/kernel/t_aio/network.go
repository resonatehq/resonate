package t_aio

import (
	"fmt"
	"net/http"

	"github.com/resonatehq/resonate/internal/announcements"
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
	announcements.Initialize(announcements.Dst)

	switch s.Kind {
	case Http:
		data := map[string]string{
			"method": s.Http.Method,
			"url":    s.Http.Url,
		}
		announcements.GetInstance().Announce(data)

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

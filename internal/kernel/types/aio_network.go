package types

import "net/http"

type NetworkKind int

const (
	Http NetworkKind = iota
)

type NetworkSubmission struct {
	Kind NetworkKind
	Http *HttpRequest
}

type NetworkCompletion struct {
	Kind NetworkKind
	Http *http.Response
}

type HttpRequest struct {
	Headers map[string]string
	Method  string
	Url     string
	Body    []byte
}

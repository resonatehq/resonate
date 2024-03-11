package queuing

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"text/template"

	"github.com/go-chi/chi/v5"
	"github.com/resonatehq/resonate/internal/util"
)

// Resonate chi router wrapper.
type (
	Router interface {
		Handle(pattern string, handler *RouteHandler)
		Match(pattern string) (*MatchResult, error)
	}

	RouterImpl struct {
		patterns *chi.Mux
		handlers map[string]*RouteHandler
	}

	RouteHandler struct {
		Connection string
		Queue      string
	}

	MatchResult struct {
		Route        string
		RoutePattern string
		Connection   string
		Queue        string
	}
)

var (
	ErrRouteDoesNotMatchAnyPattern = errors.New("route does not match any pattern")

	// Workaround for no dependency injection in coroutines. (TODO: consider alternatives)
	CRouter Router = NewRouter()
)

func CoroutineRouter() Router {
	return CRouter
}

func NewRouter() Router {
	return &RouterImpl{
		patterns: chi.NewRouter(),
		handlers: make(map[string]*RouteHandler),
	}
}

func (r *RouterImpl) Handle(pattern string, handler *RouteHandler) {
	// panic: chi: routing pattern must begin with '/' in 'resonate://demo.example.com/payments/*' [recovered]
	pattern = "/" + pattern

	r.patterns.Post(pattern, func(w http.ResponseWriter, r *http.Request) {})
	r.handlers[pattern] = handler
}

func (r *RouterImpl) Match(route string) (*MatchResult, error) {
	// panic: chi: routing pattern must begin with '/' in 'resonate://demo.example.com/payments/*' [recovered]
	route = "/" + route

	rctx := chi.NewRouteContext()

	if !r.patterns.Match(rctx, "POST", route) {
		return nil, ErrRouteDoesNotMatchAnyPattern
	}

	pattern := rctx.RoutePattern()

	handler, ok := r.handlers[pattern]
	util.Assert(ok, fmt.Sprintf("pattern %s is expected to have a registered handler", pattern))

	renderedQueue, err := r.renderQueue(handler.Queue, rctx.URLParams)
	if err != nil {
		return nil, err
	}

	return &MatchResult{
		Route:        route,
		RoutePattern: pattern,
		Connection:   handler.Connection,
		Queue:        renderedQueue,
	}, nil

}

func (r *RouterImpl) renderQueue(queueTemplate string, params chi.RouteParams) (string, error) {
	data := make(map[string]string)
	for idx, k := range params.Keys {
		data[k] = params.Values[idx]
	}

	tmpl, err := template.New("queue").Parse(queueTemplate)
	if err != nil {
		return "", err
	}

	buf := &bytes.Buffer{}
	err = tmpl.Execute(buf, data)
	if err != nil {
		return "", err
	}

	return buf.String(), nil
}

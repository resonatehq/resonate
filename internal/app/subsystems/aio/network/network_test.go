package network

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/stretchr/testify/assert"
)

func TestNetworkHttpRequest(t *testing.T) {
	r := setup()
	s := httptest.NewServer(r)
	defer s.Close()

	testCases := []struct {
		name string
		req  *types.HttpRequest
	}{
		{
			name: "get",
			req: &types.HttpRequest{
				Headers: map[string]string{
					"a": "a",
					"b": "b",
					"c": "c",
				},
				Method: "GET",
				Url:    fmt.Sprintf("%s/echo", s.URL),
			},
		},
		{
			name: "put",
			req: &types.HttpRequest{
				Headers: map[string]string{
					"a": "a",
					"b": "b",
					"c": "c",
				},
				Method: "GET",
				Url:    fmt.Sprintf("%s/echo", s.URL),
				Body:   []byte("put"),
			},
		},
		{
			name: "post",
			req: &types.HttpRequest{
				Headers: map[string]string{
					"a": "a",
					"b": "b",
					"c": "c",
				},
				Method: "GET",
				Url:    fmt.Sprintf("%s/echo", s.URL),
				Body:   []byte("post"),
			},
		},
		{
			name: "delete",
			req: &types.HttpRequest{
				Headers: map[string]string{
					"a": "a",
					"b": "b",
					"c": "c",
				},
				Method: "GET",
				Url:    fmt.Sprintf("%s/echo", s.URL),
				Body:   []byte("delete"),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sqe := &bus.SQE[types.Submission, types.Completion]{
				Submission: &types.Submission{
					Kind: types.Network,
					Network: &types.NetworkSubmission{
						Kind: types.Http,
						Http: tc.req,
					},
				},
			}

			worker := New(0).NewWorker(0)
			cqes := worker.Process([]*bus.SQE[types.Submission, types.Completion]{sqe})

			res := cqes[0].Completion.Network.Http
			assert.Equal(t, http.StatusOK, res.StatusCode)

			for key, val := range tc.req.Headers {
				assert.Equal(t, val, res.Header.Get(key))
			}

			if tc.req.Body != nil {
				body, err := io.ReadAll(res.Body)
				if err != nil {
					t.Error(err)
				}
				assert.Equal(t, tc.req.Body, body)
			}
		})
	}
}

type server struct{}

func (s *server) echo(c *gin.Context) {
	// headers
	for key, val := range c.Request.Header {
		c.Header(key, val[0])
	}

	// body
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.String(http.StatusBadRequest, "")
		return
	}

	c.String(http.StatusOK, string(body))
}

func setup() *gin.Engine {
	r := gin.Default()

	server := &server{}
	r.GET("/echo", server.echo)
	r.PUT("/echo", server.echo)
	r.POST("/echo", server.echo)
	r.DELETE("/echo", server.echo)

	return r
}

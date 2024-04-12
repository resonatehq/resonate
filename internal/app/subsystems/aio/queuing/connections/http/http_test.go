package http

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/connections/t_conn"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/metadata"
)

type (
	MockHTTPClient struct {
		HTTPClient

		expectedURL  string
		expectedBody string
		mockResponse *http.Response
	}
)

func (m *MockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	if req.Header.Get("Content-Type") != "application/json" {
		return nil, fmt.Errorf("expected application/json, got %s", req.Header.Get("Content-Type"))
	}

	if req.URL.String() != m.expectedURL {
		return nil, fmt.Errorf("expected %s, got %s", m.expectedURL, req.URL.String())
	}

	bs, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}

	if string(bs) != m.expectedBody {
		return nil, fmt.Errorf("expected %s, got %s", m.expectedBody, string(bs))
	}

	return m.mockResponse, nil
}

func TestHTTPConnection(t *testing.T) {
	tcs := []struct {
		name        string
		client      HTTPClient
		tasks       chan *t_conn.ConnectionSubmission
		config      *t_conn.ConnectionConfig
		submission  *t_conn.ConnectionSubmission
		str         string
		expectedErr error
	}{
		{
			name: "valid",
			client: &MockHTTPClient{
				expectedURL:  "http://example.com",
				expectedBody: `{"queue":"queue","taskId":"taskid","counter":1,"links":{"claim":"http://example.com/task/claim","complete":"http://example.com/task/complete"}}`,
				mockResponse: &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewBufferString("ok"))},
			},
			tasks: make(chan *t_conn.ConnectionSubmission),
			config: &t_conn.ConnectionConfig{
				Kind:     t_conn.HTTP,
				Name:     "http",
				Metadata: &metadata.Metadata{Properties: map[string]interface{}{"url": "http://example.com"}},
			},
			submission: &t_conn.ConnectionSubmission{Queue: "queue", TaskId: "taskid", Counter: 1, Links: t_conn.Links{Claim: "http://example.com/task/claim", Complete: "http://example.com/task/complete"}},
			str:        "http::http://example.com",
		},
		{
			name:   "invalid-missing-url",
			client: &MockHTTPClient{},
			tasks:  make(chan *t_conn.ConnectionSubmission),
			config: &t_conn.ConnectionConfig{
				Kind:     t_conn.HTTP,
				Name:     "http",
				Metadata: &metadata.Metadata{Properties: map[string]interface{}{"random": "http://example.com"}},
			},
			expectedErr: ErrMissingURL,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			conn := New(tc.client)

			if err := conn.Init(tc.tasks, tc.config); err != nil {
				if tc.expectedErr == nil {
					t.Fatalf("expected nil, got %v", err)
				}

				if !errors.Is(err, tc.expectedErr) {
					t.Fatalf("expected %v, got %v", tc.expectedErr, err)
				}

				return
			}

			if conn.Task() != tc.tasks {
				t.Fatalf("expected %v, got %v", tc.tasks, conn.Task())
			}

			if err := conn.Execute(tc.submission); err != nil {
				if tc.expectedErr == nil {
					t.Fatalf("expected nil, got %v", err)
				}

				if !errors.Is(err, tc.expectedErr) {
					t.Fatalf("expected %v, got %v", tc.expectedErr, err)
				}
			}

			if conn.String() != tc.str {
				t.Fatalf("expected %s, got %s", tc.str, conn.String())
			}
		})
	}
}

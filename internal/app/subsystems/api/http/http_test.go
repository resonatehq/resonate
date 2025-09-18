package http

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/test"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/stretchr/testify/assert"
)

type httpTest struct {
	*test.API
	subsystem api.Subsystem
	errors    chan error
	client    *http.Client
}

func setup(auth map[string]string) (*httpTest, error) {
	api := &test.API{}
	errors := make(chan error)
	subsystem, err := New(api, &Config{
		Addr:          ":0",
		Auth:          auth,
		Timeout:       1 * time.Second,
		TaskFrequency: 1 * time.Minute, // used as default
	}, "")

	if err != nil {
		return nil, err
	}

	// start http server
	go subsystem.Start(errors)
	time.Sleep(100 * time.Millisecond)

	return &httpTest{
		API:       api,
		subsystem: subsystem,
		errors:    errors,
		client:    &http.Client{Timeout: 1 * time.Second},
	}, nil
}

func (t *httpTest) teardown() error {
	defer close(t.errors)
	defer t.client.CloseIdleConnections()
	return t.subsystem.Stop()
}

func TestHttp(t *testing.T) {
	for _, ts := range []struct {
		name         string
		auth         map[string]string
		reqUsername  string
		reqPassword  string
		codeOverride int
	}{
		{
			name:        "BasicAuthCorrectCredentials",
			auth:        map[string]string{"user": "pass"},
			reqUsername: "user",
			reqPassword: "pass",
		},
		{
			name:         "BasicAuthIncorrectCredentials",
			auth:         map[string]string{"user": "pass"},
			reqUsername:  "user",
			reqPassword:  "notthepassword",
			codeOverride: 401,
		},
	} {
		// start the server
		httpTest, err := setup(ts.auth)
		if err != nil {
			t.Fatal(err)
		}

		t.Run(ts.name, func(t *testing.T) {
			for _, tc := range test.TestCases {
				t.Run(tc.Name, func(t *testing.T) {
					if tc.Req != nil {
						// set protocol specific header
						tc.Req.Metadata["protocol"] = "http"
					}

					httpTest.Load(t, tc.Req, tc.Res)

					req, err := http.NewRequest(tc.Http.Req.Method, fmt.Sprintf("http://%s/%s", httpTest.subsystem.Addr(), tc.Http.Req.Path), bytes.NewBuffer(tc.Http.Req.Body))
					if err != nil {
						t.Fatal(err)
					}

					// set headers
					req.Header.Set("Content-Type", "application/json")
					for key, val := range tc.Http.Req.Headers {
						req.Header.Set(key, val)
					}

					// set authorization
					if ts.auth != nil {
						req.SetBasicAuth(ts.reqUsername, ts.reqPassword)
					}

					res, err := httpTest.client.Do(req)
					if err != nil {
						t.Fatal(err)
					}
					defer util.DeferAndLog(res.Body.Close)

					body, err := io.ReadAll(res.Body)
					if err != nil {
						t.Fatal(err)
					}

					// apply override code if applicable
					code := tc.Http.Res.Code
					if ts.codeOverride != 0 {
						code = ts.codeOverride
					}

					assert.Equal(t, code, res.StatusCode, string(body))

					if tc.Http.Res.Body != nil && code >= 200 && code < 300 {
						assert.Equal(t, tc.Http.Res.Body, body)
					}

					select {
					case err := <-httpTest.errors:
						t.Fatal(err)
					default:
					}
				})
			}
		})

		// stop the server
		if err := httpTest.teardown(); err != nil {
			t.Fatal(err)
		}
	}
}

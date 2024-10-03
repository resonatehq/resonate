package client

import (
	"context"
	"encoding/base64"
	"net/http"

	v1 "github.com/resonatehq/resonate/pkg/client/v1"
)

type Client interface {
	V1() (v1.ClientWithResponsesInterface, error)
	SetServer(string)
	SetBasicAuth(string, string)
}

// Client

type client struct {
	server   string
	username string
	password string
}

func New() Client {
	return &client{}
}

func (c *client) V1() (v1.ClientWithResponsesInterface, error) {
	var opts []v1.ClientOption

	if c.username != "" && c.password != "" {
		opts = append(opts, basicAuth(c.username, c.password))
	}

	return v1.NewClientWithResponses(c.server, opts...)
}

func (c *client) SetServer(server string) {
	c.server = server
}

func (c *client) SetBasicAuth(username, password string) {
	c.username = username
	c.password = password
}

// Mock Client

type mockClient struct {
	c *v1.MockClientWithResponsesInterface
}

func MockClient(c *v1.MockClientWithResponsesInterface) Client {
	return &mockClient{c}
}

func (c *mockClient) V1() (v1.ClientWithResponsesInterface, error) {
	return c.c, nil
}

func (c *mockClient) SetServer(string) {}

func (c *mockClient) SetBasicAuth(string, string) {}

// Helper functions

func basicAuth(username, password string) v1.ClientOption {
	return func(c *v1.Client) error {
		c.RequestEditors = append(c.RequestEditors, func(ctx context.Context, req *http.Request) error {
			authHeader := "Basic " + base64.StdEncoding.EncodeToString([]byte(username+":"+password))
			req.Header.Set("Authorization", authHeader)
			return nil
		})
		return nil
	}
}

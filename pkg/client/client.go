package client

import (
	"context"
	"encoding/base64"
	"net/http"

	"github.com/resonatehq/resonate/internal/util"
	v1 "github.com/resonatehq/resonate/pkg/client/v1"
)

type Client interface {
	V1() v1.ClientWithResponsesInterface
	Setup(string) error
	SetBasicAuth(string, string)
}

// Client

type client struct {
	v1       v1.ClientWithResponsesInterface
	username string
	password string
}

func New() Client {
	return &client{}
}

func (c *client) Setup(server string) error {
	var opts []v1.ClientOption

	if c.username != "" && c.password != "" {
		opts = append(opts, basicAuth(c.username, c.password))
	}

	var err error
	c.v1, err = v1.NewClientWithResponses(server, opts...)

	return err
}

func (c *client) V1() v1.ClientWithResponsesInterface {
	util.Assert(c.v1 != nil, "v1 must not be nil")
	return c.v1
}

func (c *client) SetBasicAuth(username, password string) {
	c.username = username
	c.password = password
}

// Mock Client

type mockClient struct {
	v1 *v1.MockClientWithResponsesInterface
}

func MockClient(v1 *v1.MockClientWithResponsesInterface) Client {
	return &mockClient{v1}
}

func (c *mockClient) Setup(string) error {
	return nil
}

func (c *mockClient) V1() v1.ClientWithResponsesInterface {
	return c.v1
}

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

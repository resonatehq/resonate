package client

import (
	"context"
	"encoding/base64"
	"net/http"

	"github.com/resonatehq/resonate/pkg/client/openapi"
)

type Client openapi.ClientWithResponsesInterface

type client struct {
	openapi.ClientWithResponsesInterface
}

func New() *client {
	return &client{}
}

func (c *client) WithDefault(server string) error {
	return c.WithOpts(server)
}

func (c *client) WithBasicAuth(server, username, password string) error {
	return c.WithOpts(server, basicAuth(username, password))
}

func (c *client) WithOpts(server string, opts ...openapi.ClientOption) error {
	client, err := openapi.NewClientWithResponses(server, opts...)
	if err != nil {
		return err
	}

	c.ClientWithResponsesInterface = client
	return nil
}

func basicAuth(username, password string) openapi.ClientOption {
	return func(c *openapi.Client) error {
		c.RequestEditors = append(c.RequestEditors, func(ctx context.Context, req *http.Request) error {
			authHeader := "Basic " + base64.StdEncoding.EncodeToString([]byte(username+":"+password))
			req.Header.Set("Authorization", authHeader)
			return nil
		})
		return nil
	}
}

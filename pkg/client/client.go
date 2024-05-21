package client

import (
	"context"
	"encoding/base64"
	"net/http"

	"github.com/resonatehq/resonate/pkg/client/promises"
	"github.com/resonatehq/resonate/pkg/client/schedules"
)

type ResonateClient interface {
	PromisesV1Alpha1() promises.ClientWithResponsesInterface
	SchedulesV1Alpha1() schedules.ClientWithResponsesInterface
	SetBasicAuth(username, password string)
}

type ClientSet struct {
	Server *string

	auth *auth

	promisesV1alpha1  promises.ClientWithResponsesInterface
	schedulesV1alpha1 schedules.ClientWithResponsesInterface
}

type auth struct {
	username string
	password string
}

func NewOrDie(server *string) ResonateClient {
	return &ClientSet{
		Server: server,
	}
}

func (c *ClientSet) SetBasicAuth(username, password string) {
	c.auth = &auth{username, password}
}

func (c *ClientSet) SetPromisesV1Alpha1(client promises.ClientWithResponsesInterface) {
	c.promisesV1alpha1 = client
}

func (c *ClientSet) SetSchedulesV1Alpha1(client schedules.ClientWithResponsesInterface) {
	c.schedulesV1alpha1 = client
}

func (c *ClientSet) PromisesV1Alpha1() promises.ClientWithResponsesInterface {
	if c.promisesV1alpha1 != nil {
		return c.promisesV1alpha1
	}

	opts := []promises.ClientOption{}

	// set basic auth if provided
	if c.auth != nil {
		opts = append(opts, func(client *promises.Client) error {
			client.RequestEditors = append(client.RequestEditors, c.basicAuthRequestEditor())
			return nil
		})
	}

	var err error
	c.promisesV1alpha1, err = promises.NewClientWithResponses(*c.Server, opts...)
	if err != nil {
		panic(err)
	}

	return c.promisesV1alpha1
}

func (c *ClientSet) SchedulesV1Alpha1() schedules.ClientWithResponsesInterface {
	if c.schedulesV1alpha1 != nil {
		return c.schedulesV1alpha1
	}

	opts := []schedules.ClientOption{}

	// set basic auth if provided
	if c.auth != nil {
		opts = append(opts, func(client *schedules.Client) error {
			client.RequestEditors = append(client.RequestEditors, c.basicAuthRequestEditor())
			return nil
		})
	}

	var err error
	c.schedulesV1alpha1, err = schedules.NewClientWithResponses(*c.Server, opts...)
	if err != nil {
		panic(err)
	}
	return c.schedulesV1alpha1
}

func (c *ClientSet) basicAuthRequestEditor() func(ctx context.Context, req *http.Request) error {
	return func(ctx context.Context, req *http.Request) error {
		authHeader := "Basic " + base64.StdEncoding.EncodeToString([]byte(c.auth.username+":"+c.auth.password))
		req.Header.Set("Authorization", authHeader)

		return nil
	}
}

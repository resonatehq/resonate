package client

import (
	"github.com/resonatehq/resonate/pkg/client/promises"
	"github.com/resonatehq/resonate/pkg/client/schedules"
)

type ResonateClient interface {
	PromisesV1Alpha1() promises.ClientWithResponsesInterface
	SchedulesV1Alpha1() schedules.ClientWithResponsesInterface
}

type ClientSet struct {
	Server *string

	promisesV1alpha1  promises.ClientWithResponsesInterface
	schedulesV1alpha1 schedules.ClientWithResponsesInterface
}

func NewOrDie(server *string) ResonateClient {
	return &ClientSet{
		Server: server,
	}
}

func (c *ClientSet) SetPromisesV1Alpha1(client promises.ClientWithResponsesInterface) {
	c.promisesV1alpha1 = client
}

func (c *ClientSet) SetSchedulesV1Alpha1(client schedules.ClientWithResponsesInterface) {
	c.schedulesV1alpha1 = client
}

func (c *ClientSet) PromisesV1Alpha1() promises.ClientWithResponsesInterface {
	var err error
	c.promisesV1alpha1, err = promises.NewClientWithResponses(*c.Server)
	if err != nil {
		panic(err)
	}
	return c.promisesV1alpha1
}

func (c *ClientSet) SchedulesV1Alpha1() schedules.ClientWithResponsesInterface {
	var err error
	c.schedulesV1alpha1, err = schedules.NewClientWithResponses(*c.Server)
	if err != nil {
		panic(err)
	}
	return c.schedulesV1alpha1
}

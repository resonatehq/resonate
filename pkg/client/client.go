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
	server string

	promisesV1alpha1  promises.ClientWithResponsesInterface
	schedulesV1alpha1 schedules.ClientWithResponsesInterface
}

// convert to rest.Interface -- discovery client ?
func NewOrDie(server string) ResonateClient {
	var err error
	cs := &ClientSet{}

	cs.server = server
	cs.promisesV1alpha1, err = promises.NewClientWithResponses(server)
	if err != nil {
		panic(err)
	}
	cs.schedulesV1alpha1, err = schedules.NewClientWithResponses(server)
	if err != nil {
		panic(err)
	}

	return cs
}

func (c *ClientSet) SetPromisesV1Alpha1(client promises.ClientWithResponsesInterface) {
	c.promisesV1alpha1 = client
}

func (c *ClientSet) SetSchedulesV1Alpha1(client schedules.ClientWithResponsesInterface) {
	c.schedulesV1alpha1 = client
}

func (c *ClientSet) PromisesV1Alpha1() promises.ClientWithResponsesInterface {
	return c.promisesV1alpha1
}

func (c *ClientSet) SchedulesV1Alpha1() schedules.ClientWithResponsesInterface {
	return c.schedulesV1alpha1
}

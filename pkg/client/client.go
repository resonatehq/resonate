package client

import (
	"github.com/resonatehq/resonate/pkg/client/promises"
	"github.com/resonatehq/resonate/pkg/client/schedules"
)

type ResonateClient interface {
	PromisesV1Alpha1() promises.ClientInterface
	SchedulesV1Alpha1() schedules.ClientInterface
}

type ClientSet struct {
	api string

	promiseV1alpha1   promises.ClientInterface
	schedulesV1alpha1 schedules.ClientInterface
}

// convert to rest.Interface -- discovery client ?
func NewOrDie(api string) ResonateClient {
	var err error
	cs := &ClientSet{}

	cs.api = api
	cs.promiseV1alpha1, err = promises.NewClient(api)
	if err != nil {
		panic(err)
	}
	cs.schedulesV1alpha1, err = schedules.NewClient(api)
	if err != nil {
		panic(err)
	}

	return cs
}

func (c *ClientSet) SetPromisesV1Alpha1(client promises.ClientInterface) {
	c.promiseV1alpha1 = client
}

func (c *ClientSet) SetSchedulesV1Alpha1(client schedules.ClientInterface) {
	c.schedulesV1alpha1 = client
}

func (c *ClientSet) PromisesV1Alpha1() promises.ClientInterface {
	return c.promiseV1alpha1
}

func (c *ClientSet) SchedulesV1Alpha1() schedules.ClientInterface {
	return c.schedulesV1alpha1
}

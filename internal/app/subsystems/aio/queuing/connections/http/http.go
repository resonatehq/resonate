package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/connections/t_conn"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/metadata"
)

type (
	// HTTP is a connection to an HTTP endpoint. It implements the Connection interface and
	// is the only connection type that does not require a queue.
	HTTP struct {
		client *http.Client
		tasks  <-chan *t_conn.ConnectionSubmission
		meta   Metadata
	}

	Metadata struct {
		URL string `mapstructure:"url"`
	}

	Payload struct {
		Queue   string `json:"queue"`
		TaskId  string `json:"taskId"`
		Counter int    `json:"counter"`
		Links   Links  `json:"links"`
	}

	Links struct {
		Claim    string `json:"claim"`
		Complete string `json:"complete"`
	}
)

func New() t_conn.Connection {
	return &HTTP{}
}

func (c *HTTP) Init(tasks <-chan *t_conn.ConnectionSubmission, meta *metadata.Metadata) error {
	c.client = &http.Client{}
	c.tasks = tasks
	md := Metadata{}

	if err := metadata.Decode(meta.Properties, &md); err != nil {
		return err
	}

	c.meta = md

	return nil
}

func (c *HTTP) Task() <-chan *t_conn.ConnectionSubmission {
	return c.tasks
}

func (c *HTTP) Execute(sub *t_conn.ConnectionSubmission) error {
	// Form payload.
	payload := Payload{
		Queue:   sub.Queue,
		TaskId:  sub.TaskId,
		Counter: sub.Counter,
		Links: Links{
			Claim:    sub.Links.Claim,
			Complete: sub.Links.Complete,
		},
	}

	// Marshal payload.
	bs, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %v", err)
	}

	req, err := http.NewRequest("POST", c.meta.URL, bytes.NewBuffer(bs))
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	// Set Headers.
	req.Header.Set("Content-Type", "application/json")

	// Queue task.
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

func (c *HTTP) String() string {
	return fmt.Sprintf("http::%s", c.meta.URL)
}

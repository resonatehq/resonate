package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/connections/t_conn"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/metadata"
	"github.com/resonatehq/resonate/internal/util"
)

var (
	ErrMissingURL = fmt.Errorf("missing field 'url'")
)

type (
	// HTTP is a connection to an HTTP endpoint. It implements the Connection interface and
	// is the only connection type that does not require a queue.
	HTTP struct {
		client HTTPClient
		tasks  <-chan *t_conn.ConnectionSubmission
		meta   *Metadata
	}

	HTTPClient interface {
		Do(req *http.Request) (*http.Response, error)
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

// New creates a new connection with the type specific client.
func New(c HTTPClient) t_conn.Connection {
	return &HTTP{
		client: c,
		meta:   &Metadata{},
	}
}

// Init initializes the connection with the generic & type specific connection configuration.
func (c *HTTP) Init(tasks <-chan *t_conn.ConnectionSubmission, cfg *t_conn.ConnectionConfig) error {
	util.Assert(c.client != nil, "client must not be nil")
	util.Assert(c.meta != nil, "meta must not be nil")
	util.Assert(tasks != nil, "tasks must not be nil")
	util.Assert(cfg != nil, "config must not be nil")
	util.Assert(cfg.Metadata != nil, "metadata must not be nil")
	util.Assert(cfg.Metadata.Properties != nil, "metadata properties must not be nil")

	c.tasks = tasks

	if err := metadata.Decode(cfg.Metadata.Properties, c.meta); err != nil {
		return err
	}

	if c.meta.URL == "" {
		return fmt.Errorf("validation error for connection '%s': %w", cfg.Name, ErrMissingURL)
	}

	util.Assert(c.client != nil, "client must not be nil")
	util.Assert(c.tasks != nil, "tasks must not be nil")
	util.Assert(c.meta.URL != "", "url must not be empty")

	return nil
}

func (c *HTTP) Task() <-chan *t_conn.ConnectionSubmission {
	return c.tasks
}

func (c *HTTP) Execute(sub *t_conn.ConnectionSubmission) error {
	util.Assert(sub != nil, "submission must not be nil")
	util.Assert(sub.Queue != "", "queue must not be empty")
	util.Assert(sub.TaskId != "", "task id must not be empty")
	util.Assert(sub.Counter >= 0, "counter must be greater than or equal to 0")
	util.Assert(sub.Links.Claim != "", "claim link must not be empty")
	util.Assert(sub.Links.Complete != "", "complete link must not be empty")

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

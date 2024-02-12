package http

import (
	"bytes"
	"fmt"
	"net/http"

	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/bindings/t_bind"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/metadata"
)

type HTTP struct {
	client *http.Client
	tasks  <-chan *t_bind.BindingSubmission
	meta   Metadata
}

func New() t_bind.Binding {
	return &HTTP{}
}

func (c *HTTP) Init(tasks <-chan *t_bind.BindingSubmission, meta *metadata.Metadata) error {
	c.client = &http.Client{}
	c.tasks = tasks
	md := Metadata{}

	if err := metadata.Decode(meta.Properties, &md); err != nil {
		return err
	}

	c.meta = md

	return nil
}

func (c *HTTP) Task() <-chan *t_bind.BindingSubmission {
	return c.tasks
}

func (c *HTTP) Execute(sub *t_bind.BindingSubmission) error {
	// create request.
	payload := fmt.Sprintf(`{"taskId":"%s", "counter":%d}`, sub.TaskId, sub.Counter)

	req, err := http.NewRequest("POST", c.meta.URL, bytes.NewBuffer([]byte(payload)))
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	// set headers.
	req.Header.Set("Content-Type", "application/json")

	// make request.
	resp, err := c.client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	// print response.
	// slog.Info("response Status:", resp.Status)

	return nil
}

func (c *HTTP) String() string {
	return "http"
}

func (c *HTTP) Complete() error {
	return nil
}

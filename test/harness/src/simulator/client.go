package simulator

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	"github.com/resonatehq/resonate/test/harness/src/store"
)

type Client struct {
	ID   int
	Conn string
}

func NewClient(conn string) *Client {
	return &Client{
		ID:   rand.Intn(500),
		Conn: conn,
	}
}

func (c *Client) Open(target string) {
	c.Conn = target
}

// Invoke recieves the start of an operation and returns the end of it
func (c *Client) Invoke(op store.Operation) store.Operation {
	switch op.API {
	case store.Search:
		return c.Search(op)
	case store.Get:
		return c.Get(op)
	case store.Create:
		return c.Create(op)
	case store.Cancel:
		return c.Cancel(op)
	case store.Resolve:
		return c.Resolve(op)
	case store.Reject:
		return c.Reject(op)
	default:
		panic(fmt.Sprintf("unknown operation: %d", op.API))
	}
}

//
// implement client for durable promise specification - write/read operations
// nil for read operations
//

func (c *Client) Search(op store.Operation) store.Operation {
	return store.Operation{
		Status: store.Ok,
		API:    store.Search,
		Value:  "TODO",
	}
}

func (c *Client) Get(op store.Operation) store.Operation {
	return store.Operation{
		Status: store.Invoke,
		API:    store.Get,
		Value:  "TODO",
	}
}

func (c *Client) Create(op store.Operation) store.Operation {
	end := op

	bs, err := json.Marshal(op.Value)
	if err != nil {
		panic(err)
	}

	r, err := http.NewRequest(http.MethodPost, c.Conn+"promises/foo/create", bytes.NewBuffer(bs))
	if err != nil {
		panic(err)
	}

	r.Header.Add("Content-Type", "application/json")

	client := &http.Client{}
	res, err := client.Do(r)
	if err != nil {
		panic(err)
	}

	defer res.Body.Close()

	end.EndTime = time.Now()
	end.Status = store.Ok

	return end
}

func (c *Client) Cancel(op store.Operation) store.Operation {
	return store.Operation{
		Status: store.Invoke,
		API:    store.Cancel,
		Value:  "TODO",
	}
}

func (c *Client) Resolve(op store.Operation) store.Operation {
	return store.Operation{
		Status: store.Invoke,
		API:    store.Resolve,
		Value:  "TODO",
	}
}

func (c *Client) Reject(op store.Operation) store.Operation {
	return store.Operation{
		Status: store.Invoke,
		API:    store.Reject,
		Value:  "TODO",
	}
}

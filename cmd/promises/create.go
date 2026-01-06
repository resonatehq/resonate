package promises

import (
	"context"
	"encoding/base64"
	"errors"
	"time"

	"github.com/resonatehq/resonate/pkg/client"
	v1 "github.com/resonatehq/resonate/pkg/client/v1"
	"github.com/spf13/cobra"
)

var createPromiseExample = `
# Create a promise
resonate promises create foo --timeout 1h

# Create a promise with data and headers and tags
resonate promises create foo --timeout 1h --data foo --header bar=bar --tag baz=baz`

func CreatePromiseCmd(c client.Client) *cobra.Command {
	var (
		data    string
		timeout time.Duration
		headers map[string]string
		tags    map[string]string
	)

	cmd := &cobra.Command{
		Use:     "create <id>",
		Short:   "Create promise",
		Example: createPromiseExample,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return errors.New("must specify an id")
			}

			id := args[0]

			params := &v1.CreatePromiseParams{}

			body := v1.CreatePromiseJSONRequestBody{
				Id:      id,
				Timeout: time.Now().Add(timeout).UnixMilli(),
				Param:   &v1.Value{},
			}

			if cmd.Flag("header").Changed {
				body.Param.Headers = &headers
			}

			if cmd.Flag("data").Changed {
				encoded := base64.StdEncoding.EncodeToString([]byte(data))
				body.Param.Data = &encoded
			}

			if cmd.Flag("tag").Changed {
				body.Tags = &tags
			}

			res, err := c.V1().CreatePromiseWithResponse(context.TODO(), params, body)
			if err != nil {
				return err
			}

			if res.StatusCode() == 201 {
				cmd.Printf("Created promise: %s\n", id)
			} else if res.StatusCode() == 200 {
				cmd.Printf("Created promise: %s (deduplicated)\n", id)
			} else {
				cmd.PrintErrln(res.Status(), string(res.Body))
			}

			return nil
		},
	}

	cmd.Flags().DurationVarP(&timeout, "timeout", "t", 0, "promise timeout")
	cmd.Flags().StringToStringVar(&headers, "header", map[string]string{}, "promise param header")
	cmd.Flags().StringVar(&data, "data", "", "promise param data")
	cmd.Flags().StringToStringVar(&tags, "tag", map[string]string{}, "promise tags")

	_ = cmd.MarkFlagRequired("timeout")

	return cmd
}

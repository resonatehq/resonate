package promises

import (
	"context"
	"encoding/base64"
	"errors"
	"time"

	"github.com/resonatehq/resonate/pkg/client"
	"github.com/resonatehq/resonate/pkg/client/openapi"
	"github.com/spf13/cobra"
)

var createPromiseExample = `
# Create a promise
resonate promises create foo --timeout 1h

# Create a promise with data and headers and tags
resonate promises create foo --timeout 1h --data foo --header bar=bar --tag baz=baz`

func CreatePromiseCmd(c client.Client) *cobra.Command {
	var (
		data           string
		timeout        time.Duration
		headers        map[string]string
		tags           map[string]string
		idempotencyKey string
		strict         bool
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

			params := &openapi.CreatePromiseParams{
				Strict: &strict,
			}

			if cmd.Flag("idempotency-key").Changed {
				params.IdempotencyKey = &idempotencyKey
			}

			body := openapi.CreatePromiseJSONRequestBody{
				Id:      id,
				Timeout: time.Now().Add(timeout).UnixMilli(),
				Param:   &openapi.Value{},
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

			resp, err := c.CreatePromiseWithResponse(context.TODO(), params, body)
			if err != nil {
				return err
			}

			if resp.StatusCode() == 201 {
				cmd.Printf("Created promise: %s\n", id)
			} else if resp.StatusCode() == 200 {
				cmd.Printf("Created promise: %s (deduplicated)\n", id)
			} else {
				cmd.PrintErrln(resp.Status(), string(resp.Body))
			}

			return nil
		},
	}

	cmd.Flags().DurationVarP(&timeout, "timeout", "t", 0, "promise timeout")
	cmd.Flags().StringToStringVarP(&headers, "header", "H", map[string]string{}, "promise param header")
	cmd.Flags().StringVarP(&data, "data", "D", "", "promise param data")
	cmd.Flags().StringToStringVarP(&tags, "tag", "T", map[string]string{}, "promise tags")
	cmd.Flags().StringVarP(&idempotencyKey, "idempotency-key", "i", "", "idempotency key")
	cmd.Flags().BoolVarP(&strict, "strict", "s", true, "strict mode")

	_ = cmd.MarkFlagRequired("timeout")

	return cmd
}

package promises

import (
	"context"
	"encoding/base64"
	"time"

	"github.com/resonatehq/resonate/pkg/client"
	"github.com/resonatehq/resonate/pkg/client/promises"
	"github.com/spf13/cobra"
)

var createPromiseExample = `
# Create a promise
resonate promise create foo --timeout 1h

# Create a promise with data and headers and tags
resonate promise create foo --timeout 1h --data foo --header bar=bar --tag baz=baz`

func CreatePromiseCmd(c client.ResonateClient) *cobra.Command {
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
		Short:   "Create a durable promise",
		Example: createPromiseExample,
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 1 {
				cmd.PrintErrln("Must specify promise id")
				return
			}

			id := args[0]

			params := &promises.CreatePromiseParams{
				Strict: &strict,
			}

			if cmd.Flag("idempotency-key").Changed {
				params.IdempotencyKey = &idempotencyKey
			}

			param := &promises.PromiseValue{}

			if cmd.Flag("header").Changed {
				param.Headers = &headers
			}

			if cmd.Flag("data").Changed {
				encoded := base64.StdEncoding.EncodeToString([]byte(data))
				param.Data = &encoded
			}

			body := promises.Promise{
				Id:      id,
				Timeout: time.Now().Add(timeout).UnixMilli(),
				Param:   param,
			}

			if cmd.Flag("tag").Changed {
				body.Tags = &tags
			}

			resp, err := c.PromisesV1Alpha1().CreatePromiseWithResponse(context.TODO(), params, body)
			if err != nil {
				cmd.PrintErr(err)
				return
			}

			if resp.StatusCode() == 201 {
				cmd.Printf("Created promise: %s\n", id)
			} else if resp.StatusCode() == 200 {
				cmd.Printf("Created promise: %s (deduplicated)\n", id)
			} else {
				cmd.PrintErrln(string(resp.Body))
			}
		},
	}

	cmd.Flags().StringVarP(&data, "data", "D", "", "Promise param data")
	cmd.Flags().DurationVarP(&timeout, "timeout", "t", 0, "Promise timeout")
	cmd.Flags().StringToStringVarP(&headers, "header", "H", map[string]string{}, "Promise param header")
	cmd.Flags().StringToStringVarP(&tags, "tag", "T", map[string]string{}, "Promise tag")
	cmd.Flags().StringVarP(&idempotencyKey, "idempotency-key", "i", "", "Idempotency key")
	cmd.Flags().BoolVarP(&strict, "strict", "s", true, "Strict mode")

	_ = cmd.MarkFlagRequired("timeout")

	return cmd
}

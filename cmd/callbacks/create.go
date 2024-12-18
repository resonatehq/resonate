package callbacks

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/resonatehq/resonate/pkg/client"
	v1 "github.com/resonatehq/resonate/pkg/client/v1"
	"github.com/spf13/cobra"
)

var createCallbacksExample = `
# Create a callback
resonate callback create foo --promise-id bar --root-promise-id baz --timeout 1h --recv default

# Create a callback with url
resonate callback create foo --promise-id bar --root-promise-id baz --timeout 1h --recv poll://default/6fa89b7e-4a56-40e8-ba4e-78864caa3278

# Create a callback with object
resonate callback create foo --promise-id bar --root-promise-id baz --timeout 1h --recv {"type": "poll", "data": {"group": "default", "id": "6fa89b7e-4a56-40e8-ba4e-78864caa3278"}}
`

func CreateCallbackCmd(c client.Client) *cobra.Command {
	var (
		promiseId     string
		rootPromiseId string
		timeout       time.Duration
		recvStr       string
	)
	cmd := &cobra.Command{
		Use:     "create <id>",
		Short:   "Create callback",
		Example: createCallbacksExample,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return errors.New("must specify an id")
			}

			id := args[0]

			var recv v1.Recv

			if json.Valid([]byte(recvStr)) {
				var recv0 v1.Recv0

				if err := json.Unmarshal([]byte(recvStr), &recv0); err != nil {
					return err
				}
				if err := recv.FromRecv0(recv0); err != nil {
					return err
				}
			} else {
				if err := recv.FromRecv1(recvStr); err != nil {
					return err
				}
			}

			body := v1.CreateCallbackJSONRequestBody{
				Id:            id,
				PromiseId:     promiseId,
				RootPromiseId: rootPromiseId,
				Timeout:       time.Now().Add(timeout).UnixMilli(),
				Recv:          recv,
			}

			res, err := c.V1().CreateCallbackWithResponse(context.TODO(), nil, body)
			if err != nil {
				return err
			}

			if res.StatusCode() == 201 {
				cmd.Printf("Created callback: %s\n", id)
			} else if res.StatusCode() == 200 {
				cmd.Printf("Created callback: %s (deduplicated)\n", id)
			} else {
				cmd.PrintErrln(res.Status(), string(res.Body))
			}

			return nil
		},
	}

	cmd.Flags().StringVar(&promiseId, "promise-id", "", "promise id")
	cmd.Flags().StringVar(&rootPromiseId, "root-promise-id", "", "root promise id")
	cmd.Flags().DurationVar(&timeout, "timeout", 0, "task timeout")
	cmd.Flags().StringVar(&recvStr, "recv", "default", "task receiver")

	_ = cmd.MarkFlagRequired("promise-id")
	_ = cmd.MarkFlagRequired("root-promise-id")
	_ = cmd.MarkFlagRequired("timeout")

	return cmd
}

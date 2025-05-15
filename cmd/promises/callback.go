package promises

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/client"
	v1 "github.com/resonatehq/resonate/pkg/client/v1"
	"github.com/spf13/cobra"
)

var createPromiseCallbackExample = `
# Create a callback
resonate promises callback foo --root-promise-id bar --timeout 1h --recv default

# Create a callback with url
resonate promises callback foo --root-promise-id bar --timeout 1h --recv poll://default/1

# Create a callback with object
resonate promises callback foo --root-promise-id bar --timeout 1h --recv {"type": "poll", "data": {"group": "default", "id": "2"}}
`

func CreatePromiseCallbackCmd(c client.Client) *cobra.Command {
	var (
		rootPromiseId string
		recvStr       string
		timeout       time.Duration
	)
	cmd := &cobra.Command{
		Use:     "callback <id>",
		Short:   "Create callback",
		Example: createPromiseCallbackExample,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return errors.New("must specify a promise id")
			}

			promiseId := args[0]

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

			body := v1.CreatePromiseCallbackJSONRequestBody{
				RootPromiseId: rootPromiseId,
				Recv:          recv,
				Timeout:       time.Now().Add(timeout).UnixMilli(),
			}

			res, err := c.V1().CreatePromiseCallbackWithResponse(context.TODO(), promiseId, nil, body)
			if err != nil {
				return err
			}

			if res.StatusCode() == 201 {
				cmd.Printf("Created callback: %s\n", util.ResumeId(rootPromiseId, promiseId))
			} else if res.StatusCode() == 200 {
				if res.JSON200.Promise != nil && res.JSON200.Promise.State != v1.PromiseStatePENDING {
					cmd.Printf("Promise %s already %s\n", promiseId, strings.ToLower(string(res.JSON200.Promise.State)))
				} else {
					cmd.Printf("Created callback: %s (deduplicated)\n", util.ResumeId(rootPromiseId, promiseId))
				}
			} else {
				cmd.PrintErrln(res.Status(), string(res.Body))
			}

			return nil
		},
	}

	cmd.Flags().StringVar(&rootPromiseId, "root-promise-id", "", "root promise id")
	cmd.Flags().StringVar(&recvStr, "recv", "default", "task receiver")
	cmd.Flags().DurationVar(&timeout, "timeout", 0, "task timeout")

	_ = cmd.MarkFlagRequired("root-promise-id")
	_ = cmd.MarkFlagRequired("timeout")

	return cmd
}

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

var createPromiseSubscriptionExample = `
# Create a subscription
resonate promises subscribe foo --id bar --timeout 1h --recv default

# Create a subscription with url
resonate promises subscribe foo --id bar --timeout 1h --recv poll://default/1

# Create a subscription with object
resonate promises subscribe foo --id bar --timeout 1h --recv {"type": "poll", "data": {"group": "default", "id": "2"}}
`

func CreateSubscriptionCmd(c client.Client) *cobra.Command {
	var (
		id      string
		timeout time.Duration
		recvStr string
	)
	cmd := &cobra.Command{
		Use:     "subscribe <id>",
		Short:   "Create subscription",
		Example: createPromiseSubscriptionExample,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return errors.New("must specify an id")
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

			body := v1.CreatePromiseSubscriptionJSONRequestBody{
				Id:      id,
				Recv:    recv,
				Timeout: time.Now().Add(timeout).UnixMilli(),
			}

			res, err := c.V1().CreatePromiseSubscriptionWithResponse(context.TODO(), promiseId, nil, body)
			if err != nil {
				return err
			}

			if res.StatusCode() == 201 {
				cmd.Printf("Created subscription: %s\n", util.NotifyId(promiseId, id))
			} else if res.StatusCode() == 200 {
				if res.JSON200.Promise != nil && res.JSON200.Promise.State != v1.PromiseStatePENDING {
					cmd.Printf("Promise %s already %s\n", id, strings.ToLower(string(res.JSON200.Promise.State)))
				} else {
					cmd.Printf("Created subscription: %s (deduplicated)\n", util.NotifyId(promiseId, id))
				}
			} else {
				cmd.PrintErrln(res.Status(), string(res.Body))
			}

			return nil
		},
	}

	cmd.Flags().StringVar(&id, "id", "", "subscription id")
	cmd.Flags().StringVar(&recvStr, "recv", "default", "task receiver")
	cmd.Flags().DurationVar(&timeout, "timeout", 0, "task timeout")

	_ = cmd.MarkFlagRequired("id")
	_ = cmd.MarkFlagRequired("timeout")

	return cmd
}

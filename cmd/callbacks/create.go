package callbacks

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/resonatehq/resonate/pkg/client"
	v1 "github.com/resonatehq/resonate/pkg/client/v1"
	"github.com/spf13/cobra"
)

var createCallbacksExample = `
# Create a callback
resonate callback create --promise-id foo --root-promise-id bar --timeout 1h --recv default

# Create a callback with url
resonate callback create --promise-id foo --root-promise-id bar --timeout 1h --recv poll://default/6fa89b7e-4a56-40e8-ba4e-78864caa3278

# Create a callback with object
resonate callback create --promise-id foo --root-promise-id bar --timeout 1h --recv {"type": "poll", "data": {"group": "default", "id": "6fa89b7e-4a56-40e8-ba4e-78864caa3278"}}
`

func CreateCallbackCmd(c client.Client) *cobra.Command {
	var (
		promiseId     string
		rootPromiseId string
		timeout       time.Duration
		recv          string
	)
	cmd := &cobra.Command{
		Use:     "create ",
		Short:   "Create callbacks",
		Example: createCallbacksExample,
		RunE: func(cmd *cobra.Command, args []string) error {

			// What should requestID be?
			paramsByte, err := json.Marshal(struct {
				RequestId string `json:"request-id"`
			}{RequestId: ""})

			if err != nil {
				return nil
			}

			paramsStr := string(paramsByte)
			params := &v1.CreateCallbackParams{
				RequestId: &paramsStr,
			}

			decoder := json.NewDecoder(strings.NewReader(recv))
			var recvJson v1.Recv

			if err := decoder.Decode(&recvJson); err == nil {
				body := v1.CreateCallbackJSONRequestBody{
					PromiseId:     promiseId,
					RootPromiseId: rootPromiseId,
					Timeout:       time.Now().Add(timeout).UnixMilli(),
					Recv:          recvJson,
				}

				res, err := c.V1().CreateCallbackWithResponse(context.TODO(), params, body)

				if err != nil {
					return err
				}

				if res.StatusCode() == 201 {
					cmd.Printf("Created callback for promise: %s\n", promiseId)
				} else if res.StatusCode() == 200 {
					cmd.Printf("Callback exists for the promise: %s\n", promiseId)
				} else {
					cmd.PrintErrln(res.Status(), string(res.Body))
				}
			} else {
				// Either input is string/url or bad json input
				// Need to check for the above before making request
				// This does not work
				res, err := c.V1().CreateCallbackWithBodyWithResponse(context.TODO(), params, "application/text", strings.NewReader(recv))

				if err != nil {
					return err
				}

				if res.StatusCode() == 201 {
					cmd.Printf("Created callback for promise: %s\n", promiseId)
				} else if res.StatusCode() == 200 {
					cmd.Printf("Callback exists for the promise: %s\n", promiseId)
				} else {
					cmd.PrintErrln(res.Status(), string(res.Body))
				}
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&promiseId, "promise-id", "", "promise id")
	cmd.Flags().StringVar(&rootPromiseId, "root-promise-id", "", "root promise id")
	cmd.Flags().DurationVar(&timeout, "timeout", 0, "callback timeout")
	cmd.Flags().StringVar(&recv, "recv", "", "receive object")

	return cmd
}

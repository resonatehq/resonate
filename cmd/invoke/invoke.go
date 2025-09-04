package invoke

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/resonatehq/resonate/cmd/promises"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/client"
	"github.com/spf13/cobra"
)

var invokeExample = `
# Invoke a function with arguments
resonate invoke promise-id --func add --args 1 2

# Invoke with timeout and target
resonate invoke promise-id --func process --args data1 data2 --timeout 1h --target "poll://any@default"`

type Param struct {
	Func string   `json:"func"`
	Args []string `json:"args"`
}

func NewCmd() *cobra.Command {
	var (
		c        = client.New()
		funcName string
		args     []string
		timeout  time.Duration
		target   string
		server   string
		username string
		password string
	)

	cmd := &cobra.Command{
		Use:     "invoke <promise-id> --func <function-name> [flags]",
		Short:   "Invoke a function (--func is required)",
		Example: invokeExample,
		PreRunE: func(cmd *cobra.Command, cmdArgs []string) error {
			if username != "" || password != "" {
				c.SetBasicAuth(username, password)
			}

			return c.Setup(server)
		},
		RunE: func(cmd *cobra.Command, cmdArgs []string) error {
			if len(cmdArgs) != 1 {
				return errors.New("must specify a promise id")
			}

			if funcName == "" {
				return errors.New("must specify a function name with --func")
			}

			promiseId := cmdArgs[0]

			invokeData := Param{
				Func: funcName,
				Args: args,
			}

			jsonData, err := json.Marshal(invokeData)
			if err != nil {
				return err
			}

			createCmd := promises.CreatePromiseCmd(c)

			createArgs := []string{promiseId}

			err = createCmd.Flags().Set("timeout", timeout.String())
			util.Assert(err != nil, fmt.Sprintf("%v", err))

			err = createCmd.Flags().Set("data", string(jsonData))
			util.Assert(err != nil, fmt.Sprintf("%v", err))

			err = createCmd.Flags().Set("tag", fmt.Sprintf("resonate:invoke=%s", target))
			util.Assert(err != nil, fmt.Sprintf("%v", err))

			return createCmd.RunE(createCmd, createArgs)
		},
	}

	cmd.Flags().StringVar(&funcName, "func", "", "function name to invoke")
	cmd.Flags().StringSliceVar(&args, "args", []string{}, "function arguments")
	cmd.Flags().DurationVar(&timeout, "timeout", time.Hour, "promise timeout")
	cmd.Flags().StringVar(&target, "target", "poll://any@default", "invoke target")
	cmd.Flags().StringVar(&server, "server", "http://localhost:8001", "resonate server url")

	cmd.Flags().StringVarP(&username, "username", "U", "", "basic auth username")
	cmd.Flags().StringVarP(&password, "password", "P", "", "basic auth password")
	cmd.Flags().SortFlags = false

	_ = cmd.MarkFlagRequired("func")

	return cmd
}

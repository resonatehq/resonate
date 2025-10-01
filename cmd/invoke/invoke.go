package invoke

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/resonatehq/resonate/cmd/promises"
	"github.com/resonatehq/resonate/pkg/client"
	"github.com/spf13/cobra"
)

var invokeExample = `
# Invoke a function with arguments
resonate invoke promise-id --func add --arg 1 --arg 2 --arg "a string"

# Invoke with timeout and target
resonate invoke promise-id --func process --arg data1 --arg 5 --timeout 1h --target "poll://any@default"

# Invoke with JSON arguments
resonate invoke promise-id --func process --json-args '[{"key": "value"}, {"num": 42}]'

# Invoke with version
resonate invoke promise-id --func process --version 2 --arg data`

type Param struct {
	Func    string `json:"func"`
	Args    []any  `json:"args"`
	Version int    `json:"version"`
}

func NewCmd() *cobra.Command {
	var (
		c        = client.New()
		funcName string
		args     []string
		jsonArgs string
		timeout  time.Duration
		target   string
		server   string
		username string
		password string
		version  int
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

			if version <= 0 {
				return errors.New("version must be greater than 0")
			}

			promiseId := cmdArgs[0]

			var invokeArgs []any

			if jsonArgs != "" {
				err := json.Unmarshal([]byte(jsonArgs), &invokeArgs)
				if err != nil {
					return fmt.Errorf("failed to parse jsonArgs: %v", err)
				}
			} else {
				invokeArgs = make([]any, len(args))
				for i, arg := range args {
					var jsonObj any
					err := json.Unmarshal([]byte(arg), &jsonObj)
					if err != nil {
						// If unmarshal fails, treat as string
						invokeArgs[i] = arg
					} else {
						invokeArgs[i] = jsonObj
					}
				}
			}

			invokeData := Param{
				Func:    funcName,
				Args:    invokeArgs,
				Version: version,
			}

			jsonData, err := json.Marshal(invokeData)
			if err != nil {
				return err
			}

			createCmd := promises.CreatePromiseCmd(c)
			createArgs := []string{promiseId}

			err = createCmd.Flags().Set("timeout", timeout.String())
			if err != nil {
				return fmt.Errorf("failed to set timeout: %v", err)
			}

			err = createCmd.Flags().Set("data", string(jsonData))
			if err != nil {
				return fmt.Errorf("failed to set data: %v", err)
			}

			err = createCmd.Flags().Set("tag", fmt.Sprintf("resonate:invoke=%s", target))
			if err != nil {
				return fmt.Errorf("failed to set tag: %v", err)
			}

			return createCmd.RunE(createCmd, createArgs)
		},
	}

	cmd.Flags().StringVar(&funcName, "func", "", "function to invoke")
	cmd.Flags().StringArrayVarP(&args, "arg", "a", []string{}, "function argument, can be provided multiple times")
	cmd.Flags().StringVar(&jsonArgs, "json-args", "", "function arguments as json array")
	cmd.Flags().DurationVar(&timeout, "timeout", time.Hour, "promise timeout")
	cmd.Flags().StringVar(&target, "target", "poll://any@default", "invoke target")
	cmd.Flags().StringVar(&server, "server", "http://localhost:8001", "resonate server url")
	cmd.Flags().StringVarP(&username, "username", "U", "", "basic auth username")
	cmd.Flags().StringVarP(&password, "password", "P", "", "basic auth password")
	cmd.Flags().IntVar(&version, "version", 1, "function version")

	cmd.Flags().SortFlags = false
	_ = cmd.MarkFlagRequired("func")

	return cmd
}

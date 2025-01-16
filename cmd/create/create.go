package create

import (
	"errors"
	"fmt"
	"strings"

	"github.com/resonatehq/resonate/pkg/client"
	"github.com/spf13/cobra"
)

var (
	// cmd
	name string // name of the project
	sdk  string // type of the project

	// auth
	c        = client.New()
	server   string
	username string
	password string
)

func NewCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "create",
		Short:   "Create a new Resonate project",
		Example: exampleCMD,
		// TODO - is this really needed at this time?
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if username != "" || password != "" {
				c.SetBasicAuth(username, password)
			}

			return c.Setup(server)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := validate(sdk, name); err != nil {
				return err
			}

			if err := scaffold(sdk, name); err != nil {
				return err
			}

			return nil
		},
	}

	cmd.Flags().StringVarP(&name, "name", "n", "", "Name of the project (required)")
	cmd.Flags().StringVarP(&sdk, "sdk", "s", "python", "SDK to use (e.g., python, typescript)")

	_ = cmd.MarkFlagRequired("name")
	_ = cmd.MarkFlagRequired("sdk")

	return cmd
}

func validate(sdk, name string) error {
	if name == "" {
		return errors.New("project name is required")
	}

	if sdk == "" {
		return errors.New("sdk type is required")
	}

	if !isSupported(sdk) {
		return fmt.Errorf("unsupported sdk type. supported sdks are: %s", strings.Join(SDKs, ", "))
	}

	return nil
}

func isSupported(sdk string) bool {
	for _, supported := range SDKs {
		if sdk == supported {
			return true
		}
	}

	return false
}

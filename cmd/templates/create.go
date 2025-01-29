package templates

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func CreateTemplateCmd() *cobra.Command {
	var (
		// cmd
		name string // name of the project
		sdk  string // type of the project
	)

	// TODO - I think the --sdk will replace by the template type like py, flask, nestjs, expressjs etc.
	// And more extend to the each template with the multiple types like simple-app gateway-app etc.
	// NOTE - The name represents the desired project name for a template.
	exampleCMD := `
	# Create Resonate project
	resonate templates create --name my-app --sdk py
	
	OR

	# Create Resonate project
	resonate templates create -n my-app -s py
	`

	cmd := &cobra.Command{
		Use:     "create",
		Short:   "Create a new Resonate project",
		Example: exampleCMD,
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
	cmd.Flags().StringVarP(&sdk, "sdk", "s", "python", "SDK to use (e.g., py, ts)")

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

	err := checkProjectExists(name)
	if err != nil {
		return err
	}

	return nil
}

func checkProjectExists(name string) error {
	info, err := os.Stat(name)

	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}

		return err
	}

	if info.IsDir() {
		return fmt.Errorf("project named '%s' already exists", name)
	}

	return nil
}

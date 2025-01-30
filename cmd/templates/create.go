package templates

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func CreateTemplateCmd() *cobra.Command {
	var (
		name     string
		template string
	)

	exampleCMD := `
	# Create Resonate project
	resonate templates create --name my-app --template py
	
	OR

	# Create Resonate project
	resonate templates create -n my-app -t py
	`

	cmd := &cobra.Command{
		Use:     "create",
		Short:   "Create a new Resonate project",
		Example: exampleCMD,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := validate(template, name); err != nil {
				return err
			}

			if err := scaffold(template, name); err != nil {
				return err
			}

			return nil
		},
	}

	cmd.Flags().StringVarP(&name, "name", "n", "", "Name of the project (required).")
	cmd.Flags().StringVarP(&template, "template", "t", "", "Name of the template (required). Run 'resonate templates list' to view available templates.")

	_ = cmd.MarkFlagRequired("name")
	_ = cmd.MarkFlagRequired("template")

	return cmd
}

func validate(template, name string) error {
	if name == "" {
		return errors.New("project name is required")
	}

	if template == "" {
		return errors.New("template name is required")
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

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
	resonate templates create --name my-app --template py
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

			fmt.Printf("\nproject successfully created in folder %s\n", name)
			return nil
		},
	}

	cmd.Flags().StringVarP(&name, "name", "n", "", "name of the project")
	cmd.Flags().StringVarP(&template, "template", "t", "", "name of the template, run 'resonate templates list' to view available templates")

	_ = cmd.MarkFlagRequired("name")
	_ = cmd.MarkFlagRequired("template")

	return cmd
}

func validate(template, name string) error {
	if name == "" {
		return errors.New("a folder name is required")
	}

	if template == "" {
		return errors.New("template name is required")
	}

	err := checkFolderExists(name)
	if err != nil {
		return err
	}

	return nil
}

func checkFolderExists(name string) error {
	info, err := os.Stat(name)

	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}

		return err
	}

	if info.IsDir() {
		return fmt.Errorf("a folder named '%s' already exists", name)
	}

	return nil
}

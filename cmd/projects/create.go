package projects

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func CreateProjectCmd() *cobra.Command {
	var (
		name     string
		template string
	)

	exampleCMD := `
resonate projects create --template basic-workflow-py
resonate projects create --template basic-workflow-py --name my-app`

	cmd := &cobra.Command{
		Use:     "create",
		Short:   "Create a new resonate project",
		Example: exampleCMD,
		RunE: func(cmd *cobra.Command, args []string) error {
			if name == "" {
				name = template
			}

			if err := validate(name); err != nil {
				return err
			}

			if err := scaffold(template, name); err != nil {
				return err
			}

			cmd.Printf("Template '%s' successfully copied to folder '%s'\n", template, name)
			return nil
		},
	}

	cmd.Flags().StringVarP(&template, "template", "t", "", "id of the template, run 'resonate project list' to view available templates")
	cmd.Flags().StringVarP(&name, "name", "n", "", "name of the project")

	_ = cmd.MarkFlagRequired("template")

	return cmd
}

func validate(name string) error {
	return checkFolderExists(name)
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

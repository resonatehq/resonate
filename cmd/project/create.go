package project

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func CreateProjectCmd() *cobra.Command {
	var (
		name    string
		project string
	)

	exampleCMD := `
		resonate project create --name my-app --project py
		resonate project create -n my-app -p py
	`

	cmd := &cobra.Command{
		Use:     "create",
		Short:   "Create a new resonate application node project",
		Example: exampleCMD,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := validate(project, name); err != nil {
				return err
			}

			if err := scaffold(project, name); err != nil {
				return err
			}

			fmt.Printf("\nproject successfully created in folder %s\n", name)
			return nil
		},
	}

	cmd.Flags().StringVarP(&name, "name", "n", "", "name of the project")
	cmd.Flags().StringVarP(&project, "project", "p", "", "name of the project, run 'resonate project list' to view available projects")

	_ = cmd.MarkFlagRequired("name")
	_ = cmd.MarkFlagRequired("project")

	return cmd
}

func validate(project, name string) error {
	if name == "" {
		return errors.New("a folder name is required")
	}

	if project == "" {
		return errors.New("project name is required")
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

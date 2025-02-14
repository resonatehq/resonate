package projects

import (
	"fmt"

	"github.com/spf13/cobra"
)

func ListProjectCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Short:   "List resonate projects",
		Example: "resonate project list",
		RunE: func(cmd *cobra.Command, args []string) error {
			templates, err := GetProjects()
			if err != nil {
				return err
			}

			display(templates)
			return nil
		},
	}

	return cmd
}

func display(templates Projects) {
	for name, t := range templates {
		fmt.Printf("%s:\n\t%s\n", name, t.Desc)
	}
}

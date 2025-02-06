package templates

import (
	"fmt"

	"github.com/spf13/cobra"
)

func ListTemplateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Short:   "List the available application node projects",
		Example: "resonate project list",
		RunE: func(cmd *cobra.Command, args []string) error {
			templates, err := GetTemplates()
			if err != nil {
				return err
			}

			display(templates)
			return nil
		},
	}

	return cmd
}

func display(templates Templates) {
	for name, t := range templates {
		fmt.Printf("\n%s\n\t%s\n", name, t.Desc)
	}
}

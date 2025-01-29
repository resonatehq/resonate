package templates

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

func ListTemplateCmd() *cobra.Command {
	exampleCMD := `
	# Create Resonate project
	resonate templates list
	`

	cmd := &cobra.Command{
		Use:     "list",
		Short:   "List the available template projects",
		Example: exampleCMD,
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
	writer := tabwriter.NewWriter(os.Stdout, 20, 0, 1, ' ', 0)

	fmt.Fprintln(writer, "\n| Templates          | Description                |")
	fmt.Fprintln(writer, "|--------------------|----------------------------|")

	for name, t := range templates {
		fmt.Fprintf(writer, "| %-18s | %-26s |\n", name, t.Desc)
	}

	writer.Flush()
}

package templates

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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
			// TODO - use some sort of struct and display the name of the template, desc and the supported sdk.
			templates, err := fetch()
			if err != nil {
				return err
			}

			display(templates)
			return nil
		},
	}

	return cmd
}

func fetch() (map[string]map[string]string, error) {
	const url = "https://raw.githubusercontent.com/resonatehq/templates/refs/heads/main/templates.json"

	res, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if err := checkstatus(res); err != nil {
		return nil, err
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	templates, err := parse(body)
	if err != nil {
		return nil, err
	}

	return templates, nil
}

func parse(body []byte) (map[string]map[string]string, error) {
	var templates map[string]map[string]string
	if err := json.Unmarshal(body, &templates); err != nil {
		return nil, err
	}

	return templates, nil
}

func display(templates map[string]map[string]string) {
	writer := tabwriter.NewWriter(os.Stdout, 20, 0, 1, ' ', 0)

	fmt.Fprintln(writer, "\n| Templates          | Description                |")
	fmt.Fprintln(writer, "|--------------------|----------------------------|")

	for name, t := range templates {
		if desc, exists := t["desc"]; exists {
			fmt.Fprintf(writer, "| %-18s | %-26s |\n", name, desc)
		}
	}

	writer.Flush()
}

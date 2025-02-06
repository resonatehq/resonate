package templates

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/spf13/cobra"
)

type (
	Template struct {
		Href string `json:"href"`
		Desc string `json:"desc"`
	}

	Templates map[string]Template
)

func NewCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "project",
		Aliases: []string{"project"},
		Short:   "Resonate application node projects",
		Run: func(cmd *cobra.Command, args []string) {
			_ = cmd.Help()
		},
	}

	// Add subcommands
	cmd.AddCommand(ListTemplateCmd())   // list available templates
	cmd.AddCommand(CreateTemplateCmd()) // create a template project

	return cmd
}

func GetTemplates() (Templates, error) {
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

func parse(body []byte) (Templates, error) {
	templates := Templates{}
	if err := json.Unmarshal(body, &templates); err != nil {
		return nil, err
	}

	return templates, nil
}

func GetTemplateKeys(templates Templates) []string {
	keys := make([]string, 0)

	for name := range templates {
		keys = append(keys, name)
	}

	return keys
}

package projects

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/spf13/cobra"
)

type (
	Project struct {
		Href string `json:"href"`
		Desc string `json:"desc"`
	}

	Projects map[string]Project
)

func NewCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "projects",
		Aliases: []string{"project"},
		Short:   "Resonate projects",
		Run: func(cmd *cobra.Command, args []string) {
			_ = cmd.Help()
		},
	}

	// Add subcommands
	cmd.AddCommand(ListProjectCmd())   // list projects
	cmd.AddCommand(CreateProjectCmd()) // create a project

	return cmd
}

func GetProjects() (Projects, error) {
	const url = "https://raw.githubusercontent.com/resonatehq/templates/refs/heads/main/templates.json"

	res, err := http.Get(url)
	if err != nil {
		return nil, err
	}

	if err := checkstatus(res); err != nil {
		return nil, err
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	if err := res.Body.Close(); err != nil {
		return nil, err
	}

	projects, err := parse(body)
	if err != nil {
		return nil, err
	}

	return projects, nil
}

func parse(body []byte) (Projects, error) {
	projects := Projects{}
	if err := json.Unmarshal(body, &projects); err != nil {
		return nil, err
	}

	return projects, nil
}

func GetProjectKeys(projects Projects) []string {
	keys := make([]string, 0)

	for name := range projects {
		keys = append(keys, name)
	}

	return keys
}

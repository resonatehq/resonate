package projects

import (
	"strings"

	"github.com/spf13/cobra"
)

func ListProjectCmd() *cobra.Command {
	var lang string
	var python, typescript bool

	cmd := &cobra.Command{
		Use:     "list",
		Short:   "List resonate projects",
		Example: "resonate project list [--python|--typescript]",
		RunE: func(cmd *cobra.Command, args []string) error {
			if python {
				lang = "python"
			}
			if typescript {
				lang = "typescript"
			}
			templates, err := GetProjects()
			if err != nil {
				return err
			}

			if lang != "" {
				templates = filterByLang(templates, lang)
			}

			display(cmd, templates)
			return nil
		},
	}

	cmd.Flags().StringVar(&lang, "lang", "", "filter templates by language: python or typescript")
	cmd.Flags().BoolVar(&python, "python", false, "show only python templates")
	cmd.Flags().BoolVar(&typescript, "typescript", false, "show only typescript templates")

	return cmd
}

func display(cmd *cobra.Command, templates Projects) {
	cmd.Printf("\n✨ Available templates ✨\n\n")
	for name, t := range templates {
		cmd.Printf("✨ %s\n\n\t%s\n\n", name, t.Desc)
		cmd.Printf("\tTo use this template, run:\n\n")
		cmd.Printf("\tresonate project create --name your-project --template %s\n\n", name)
	}
}

func filterByLang(projects Projects, lang string) Projects {
	filtered := Projects{}
	for name, p := range projects {
		if strings.EqualFold(p.Lang, lang) {
			filtered[name] = p
		}
	}
	return filtered
}

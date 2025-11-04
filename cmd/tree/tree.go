package tree

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/resonatehq/resonate/pkg/client"
	v1 "github.com/resonatehq/resonate/pkg/client/v1"
	"github.com/spf13/cobra"
)

type searchParams struct {
	id     string
	root   string
	cursor *string
}

func NewCmd() *cobra.Command {
	var (
		c        = client.New()
		server   string
		username string
		password string
	)

	cmd := &cobra.Command{
		Use:     "tree <id>",
		Aliases: []string{"tree"},
		Short:   "Resonate call graph tree",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if username != "" || password != "" {
				c.SetBasicAuth(username, password)
			}

			return c.Setup(server)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return errors.New("must specify an id")
			}

			searches := []*searchParams{{id: "*", root: args[0]}}
			promises := map[string][]v1.Promise{}

			for len(searches) > 0 {
				params := searches[0]
				searches = searches[1:]

				res, err := c.V1().SearchPromisesWithResponse(context.TODO(), &v1.SearchPromisesParams{
					Id:     &params.id,
					Tags:   &map[string]string{"resonate:root": params.root},
					Cursor: params.cursor,
				})
				if err != nil {
					return err
				}
				if res.StatusCode() != 200 {
					cmd.PrintErrln(res.Status(), string(res.Body))
					return nil
				}

				for _, p := range *res.JSON200.Promises {
					if parent, ok := p.Tags["resonate:parent"]; ok && parent != p.Id {
						promises[parent] = append(promises[parent], p)
					}

					if p.Tags["resonate:scope"] == "global" && p.Id != params.root {
						searches = append(searches, &searchParams{
							id:     "*",
							root:   p.Id,
							cursor: nil,
						})
					}
				}

				if res.JSON200.Cursor != nil {
					searches = append(searches, &searchParams{
						id:     params.id,
						root:   params.root,
						cursor: res.JSON200.Cursor,
					})
				}
			}

			for _, children := range promises {
				sort.Slice(children, func(i, j int) bool { return *children[i].CreatedOn < *children[j].CreatedOn })
			}

			var printTree func(id string, prefix string, postfix string, isLast bool, isRoot bool)
			printTree = func(id string, prefix string, postfix string, isLast bool, isRoot bool) {
				var connector string

				if isRoot {
					connector = ""
				} else if !isLast {
					connector = "├── "
				} else {
					connector = "└── "
				}

				// Print the current node
				cmd.Printf("%s%s%s %s\n", prefix, connector, id, postfix)

				// Recurse into children
				children := promises[id]
				for i, child := range children {
					var newPrefix string
					var newPostfix string

					if isRoot {
						newPrefix = prefix
					} else if !isLast {
						newPrefix = prefix + "│   "
					} else {
						newPrefix = prefix + "    "
					}

					var f string
					if child.Tags["resonate:func"] != "" {
						f = " " + child.Tags["resonate:func"]
					}
					if child.Tags["resonate:timeout"] != "" {
						f = " sleep"
					}

					if child.Tags["resonate:scope"] == "global" {
						newPostfix = fmt.Sprintf("(rpc%s)", f)
					} else {
						newPostfix = fmt.Sprintf("(run%s)", f)
					}

					printTree(child.Id, newPrefix, newPostfix, i == len(children)-1, false)
				}
			}

			printTree(args[0], "", "", true, true)
			return nil
		},
	}

	// Flags
	cmd.PersistentFlags().StringVarP(&server, "server", "", "http://localhost:8001", "resonate url")
	cmd.PersistentFlags().StringVarP(&username, "username", "U", "", "basic auth username")
	cmd.PersistentFlags().StringVarP(&password, "password", "P", "", "basic auth password")

	return cmd
}

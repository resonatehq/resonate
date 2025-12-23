package tree

import (
	"context"
	"encoding/base64"
	"encoding/json"
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
		token    string
	)

	cmd := &cobra.Command{
		Use:     "tree <id>",
		Aliases: []string{"tree"},
		Short:   "Resonate call graph tree",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if username != "" || password != "" {
				c.SetBasicAuth(username, password)
			}
			if token != "" {
				c.SetBearerToken(token)
			}

			return c.Setup(server)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return errors.New("must specify an id")
			}

			var rootPromise *v1.Promise
			searches := []*searchParams{{id: "*", root: args[0]}}
			promises := map[string][]*v1.Promise{}

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
					if p.Id == args[0] {
						rootPromise = &p
					}
					if parent, ok := p.Tags["resonate:parent"]; ok && parent != p.Id {
						promises[parent] = append(promises[parent], &p)
					}

					if p.Tags["resonate:scope"] == "global" && p.Id != params.root {
						searches = append(searches, &searchParams{id: "*", root: p.Id})
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

			for _, children := range promises { // nosemgrep: range-over-map
				sort.Slice(children, func(i, j int) bool { return *children[i].CreatedOn < *children[j].CreatedOn })
			}

			var printTree func(promise *v1.Promise, prefix string, isLast bool, isRoot bool)
			printTree = func(promise *v1.Promise, prefix string, isLast bool, isRoot bool) {
				var connector string
				if isRoot {
					connector = ""
				} else if !isLast {
					connector = "├── "
				} else {
					connector = "└── "
				}

				var status string
				switch promise.State {
				case v1.PromiseStatePENDING:
					status = " 🟡"
				case v1.PromiseStateRESOLVED:
					status = " 🟢"
				case v1.PromiseStateREJECTED, v1.PromiseStateREJECTEDCANCELED, v1.PromiseStateREJECTEDTIMEDOUT:
					status = " 🔴"
				}

				// Print the current node
				cmd.Printf("%s%s%s%s %s\n", prefix, connector, promise.Id, status, details(promise))

				// Recurse into children
				children := promises[promise.Id]
				for i, child := range children {
					var newPrefix string

					if isRoot {
						newPrefix = prefix
					} else if !isLast {
						newPrefix = prefix + "│   "
					} else {
						newPrefix = prefix + "    "
					}

					printTree(child, newPrefix, i == len(children)-1, false)
				}
			}

			var promise *v1.Promise
			if rootPromise != nil {
				promise = rootPromise
			} else {
				promise = &v1.Promise{Id: args[0]}
			}

			printTree(promise, "", true, true)
			return nil
		},
	}

	// Flags
	cmd.Flags().StringVarP(&server, "server", "S", "http://localhost:8001", "resonate server url")
	cmd.Flags().StringVarP(&token, "token", "T", "", "JWT bearer token")
	cmd.Flags().StringVarP(&username, "username", "U", "", "basic auth username")
	cmd.Flags().StringVarP(&password, "password", "P", "", "basic auth password")

	return cmd
}

func details(p *v1.Promise) string {
	if p == nil {
		return ""
	}

	var f string
	var details string

	if p.Param.Data != nil {
		if b, err := base64.StdEncoding.DecodeString(*p.Param.Data); err == nil {
			var d map[string]any
			if err := json.Unmarshal(b, &d); err == nil {
				f, _ = d["func"].(string)
			}

			if f != "" {
				f = " " + f
			}
		}
	}

	if p.Tags["resonate:timeout"] != "" {
		details = "(sleep)"
	} else if p.Tags["resonate:scope"] == "global" {
		details = fmt.Sprintf("(rpc%s)", f)
	} else if p.Tags["resonate:scope"] == "local" {
		details = fmt.Sprintf("(run%s)", f)
	}

	return details
}

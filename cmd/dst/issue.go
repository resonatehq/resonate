package dst

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	netHttp "net/http"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

type Issue struct {
	Title string `json:"title"`
	Body  string `json:"body"`
}

const issueFmt = `# DST Failed
%s

**Seed**
~~~
%d
~~~

**Scenario**
~~~
%s
~~~

**Store**
~~~
%s
~~~

**Commit**
~~~
%s
~~~

**Command**
~~~
go run ./... dst run --seed %d --ticks %d --scenario %s --aio-store %s
~~~

**Logs**
~~~
%s
~~~

[more details](%s)
`

func CreateDSTIssueCmd() *cobra.Command {
	var (
		seed     int64
		ticks    int64
		scenario string
		store    string
		reason   string
		file     string
		repo     string
		commit   string
		url      string
	)

	cmd := &cobra.Command{
		Use:   "issue",
		Short: "Create a GitHub issue for failed dst run",
		RunE: func(cmd *cobra.Command, args []string) error {
			token, ok := os.LookupEnv("GITHUB_TOKEN")
			if !ok {
				return fmt.Errorf("github token not set")
			}

			// read logs file
			logs := "n/a"
			if file != "" {
				var err error
				logs, err = parseLogs(file, 50, 100)
				if err != nil {
					return fmt.Errorf("failed to parse logs")
				}
			}

			// create github issue
			issue := &Issue{
				Title: fmt.Sprintf("DST: %d", seed),
				Body:  fmt.Sprintf(issueFmt, reason, seed, scenario, store, commit, seed, ticks, scenario, store, logs, url),
			}

			return createGitHubIssue(repo, token, issue)
		},
	}

	cmd.Flags().Int64Var(&seed, "seed", 0, "dst seed")
	cmd.Flags().Int64Var(&ticks, "ticks", 1000, "dst ticks")
	cmd.Flags().StringVar(&scenario, "scenario", "", "dst scenario")
	cmd.Flags().StringVar(&store, "store", "", "dst store")
	cmd.Flags().StringVar(&reason, "reason", "", "dst failure reason")
	cmd.Flags().StringVar(&file, "file", "", "dst logs file")
	cmd.Flags().StringVar(&repo, "repo", "", "github repo")
	cmd.Flags().StringVar(&commit, "commit", "", "git commit sha")
	cmd.Flags().StringVar(&url, "url", "", "github action url")

	return cmd
}

func parseLogs(filename string, head int, tail int) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return "", err
	}

	var result strings.Builder

	scanner := bufio.NewScanner(file)
	lineCount := 0
	lastLines := make([]string, 0, tail)

	for scanner.Scan() {
		line := scanner.Text()
		lineCount++

		if lineCount <= head {
			result.WriteString(line)
			result.WriteString("\n")
		}

		lastLines = append(lastLines, line)
		if len(lastLines) > tail {
			lastLines = lastLines[1:]
		}
	}

	if lineCount > tail {
		result.WriteString("\n...\n\n")
		for _, line := range lastLines {
			result.WriteString(line)
			result.WriteString("\n")
		}
	}

	if err := file.Close(); err != nil {
		return "", err
	}

	return result.String(), nil
}

func createGitHubIssue(repo string, token string, issue *Issue) error {
	url := fmt.Sprintf("https://api.github.com/repos/%s/issues", repo)

	// Convert issue to JSON
	payload, err := json.Marshal(issue)
	if err != nil {
		return err
	}

	// Create HTTP request
	req, err := netHttp.NewRequest("POST", url, bytes.NewBuffer(payload))
	if err != nil {
		return err
	}

	// Set required headers
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	req.Header.Set("Content-Type", "application/json")

	// Send the HTTP request
	client := netHttp.DefaultClient
	res, err := client.Do(req)
	if err != nil {
		return err
	}

	// Check the response status
	if res.StatusCode != netHttp.StatusCreated {
		return fmt.Errorf("failed to create GitHub issue, status code: %d", res.StatusCode)
	}

	return res.Body.Close()
}

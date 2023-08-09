package cmd

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

const issueFmt = ` # DST Failed
**Seed**
~~~
%d
~~~

**Commit**
~~~
%s
~~~

**Command**
~~~
SEED=%d go test -v --count=1 --timeout=60m -run ^TestDST$ ./...
~~~

**Logs**
~~~
SEED=%d go test -v --count=1 --timeout=60m -run ^TestDST$ ./...

%s
~~~
`

var (
	seed   int64
	file   string
	repo   string
	commit string
)

type Issue struct {
	Title string `json:"title"`
	Body  string `json:"body"`
}

func parseLogs(filename string, head int, tail int) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer file.Close()

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
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(payload))
	if err != nil {
		return err
	}

	// Set required headers
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	req.Header.Set("Content-Type", "application/json")

	// Send the HTTP request
	client := http.DefaultClient
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	// Check the response status
	if res.StatusCode != http.StatusCreated {
		return fmt.Errorf("failed to create GitHub issue, status code: %d", res.StatusCode)
	}

	return nil
}

var dstCmd = &cobra.Command{
	Use:   "dst",
	Short: "Create an issue with failed dst information",
	RunE: func(cmd *cobra.Command, args []string) error {
		token, ok := os.LookupEnv("GITHUB_TOKEN")
		if !ok {
			return fmt.Errorf("github token not set")
		}

		// read logs file
		logs, err := parseLogs(file, 50, 100)
		if err != nil {
			return fmt.Errorf("failed to parse logs")
		}

		// create github issue
		issue := &Issue{
			Title: fmt.Sprintf("DST: %d", seed),
			Body:  fmt.Sprintf(issueFmt, seed, commit, seed, seed, logs),
		}

		return createGitHubIssue(repo, token, issue)
	},
}

func init() {
	dstCmd.Flags().Int64VarP(&seed, "seed", "", 0, "dst seed")
	dstCmd.Flags().StringVarP(&file, "file", "", "", "dst logs file")
	dstCmd.Flags().StringVarP(&repo, "repo", "", "", "github repo")
	dstCmd.Flags().StringVarP(&commit, "commit", "", "", "git commit sha")

	rootCmd.AddCommand(dstCmd)
}

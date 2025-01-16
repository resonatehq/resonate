package create

import (
	"fmt"
	"os/exec"
)

var (
	exampleCMD = `# Create Resonate project
	resonate create --name my-python-app --sdk python`

	// valiate the user input sdk with the supported list
	// TODO - may convert to map
	// TODO - we cal also use the struct which combine the sdk and repo urls
	/*
		type SdkRepo struct {
		    Name    string
		    RepoURL string
		}
	*/
	SDKs = []string{"python", "ts"}

	// Repos
	Repos = map[string]string{
		"python": "https://github.com/webpy/webpy-examples.git", // FIXME - will replace with the resonate hq url
	}
)

func scaffold(sdk, name string) error {
	url, err := getRepositoryURL(sdk)
	if err != nil {
		return err
	}

	if err := cloneRepository(url, name); err != nil {
		return err
	}

	return nil
}

// TODO - make the repo public or do somthing with the ssh which will access to private repos as well.
// NOTE - working only on public repos.
func getRepositoryURL(sdk string) (string, error) {
	url, ok := Repos[sdk]
	if !ok {
		return "", fmt.Errorf("unsupported sdk: %s", sdk)
	}

	return url, nil
}

// TODO - clone the same name repo only once need to handle either duplicate or return already exist project name to user
func cloneRepository(url, name string) error {
	cmd := exec.Command("git", "clone", url, name)
	if err := cmd.Run(); err != nil {
		return err
	}

	return nil
}

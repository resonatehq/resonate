package projects

import (
	"archive/zip"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/resonatehq/resonate/internal/util"
)

// scaffold orchestrates the setup of the project from source to destination.
func scaffold(tmpl string, name string) error {
	projects, err := GetProjects()
	if err != nil {
		return err
	}

	// find the project based on template ID
	project, exists := projects[tmpl]
	if !exists {
		return fmt.Errorf("unknown project '%s', available projects are: %v", tmpl, GetProjectKeys(projects))
	}

	if err := setup(project.Href, name); err != nil {
		return err
	}

	return nil
}

// setup downloads and unzips the project to the destination folder.
func setup(url, dest string) error {
	tmp := dest + ".zip"
	if err := download(url, tmp); err != nil {
		return err
	}
	defer util.DeferAndLog(func() error { return os.Remove(tmp) })

	if err := unzip(tmp, dest); err != nil {
		return err
	}

	return nil
}

// download fetches a file from the URL and stores it locally.
func download(url, file string) error {
	res, err := http.Get(url)
	if err != nil {
		return err
	}
	defer util.DeferAndLog(res.Body.Close)

	if err := checkstatus(res); err != nil {
		return err
	}

	out, err := os.Create(file)
	if err != nil {
		return err
	}
	defer util.DeferAndLog(out.Close)

	_, err = io.Copy(out, res.Body)
	return err
}

// checkstatus verifies the HTTP response for a successful status.
func checkstatus(res *http.Response) error {
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to fetch project: %s", res.Status)
	}

	return nil
}

// unzip extracts the contents of a zip file to the destination folder.
func unzip(src, dest string) error {
	r, err := zip.OpenReader(src)
	if err != nil {
		return err
	}
	defer util.DeferAndLog(r.Close)

	root, err := extract(r, dest)
	if err != nil {
		return err
	}

	if root != "" {
		path := filepath.Join(dest, root)
		return restructure(path, dest)
	}

	return nil
}

// extract unzips the contents and returns the root folder name.
func extract(r *zip.ReadCloser, dest string) (string, error) {
	var root string
	for _, f := range r.File {
		rel := strings.TrimPrefix(f.Name, root)
		file := filepath.Join(dest, rel)

		if root == "" {
			root = base(f.Name)
		}

		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(file, os.ModePerm); err != nil {
				return "", err
			}
			continue
		}

		if err := os.MkdirAll(filepath.Dir(file), os.ModePerm); err != nil {
			return "", err
		}

		if err := write(f, file); err != nil {
			return "", err
		}
	}

	return root, nil
}

// base returns the root directory name from a path.
func base(name string) string {
	parts := strings.Split(name, "/")
	if len(parts) > 0 {
		return parts[0]
	}

	return ""
}

// write writes a file from a zip entry to the destination path.
func write(f *zip.File, path string) error {
	out, err := os.Create(path)
	if err != nil {
		return err
	}
	defer util.DeferAndLog(out.Close)

	rc, err := f.Open()
	if err != nil {
		return err
	}
	defer util.DeferAndLog(rc.Close)

	_, err = io.Copy(out, rc)
	return err
}

// restructure moves extracted contents from a root directory to destination.
func restructure(src, dest string) error {
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if err := move(src, dest, entry); err != nil {
			return err
		}
	}

	return os.Remove(src)
}

// move moves a file or directory from the source to the destination
func move(src, dest string, entry os.DirEntry) error {
	old := filepath.Join(src, entry.Name())
	new := filepath.Join(dest, entry.Name())

	return os.Rename(old, new)
}

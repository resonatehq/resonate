package templates

import (
	"archive/zip"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

var (
	// TODO - will remove the repos and sdks and fetch from the templates.json, and comapre with the incoming type by user.
	sdks = []string{"py", "ts"}

	// repos
	repos = map[string]string{
		"py": "https://github.com/resonatehq/scaffold-py/archive/refs/heads/main.zip",
	}
)

// scaffold orchestrates the setup of the SDK from source to destination.
func scaffold(sdk, name string) error {
	url, err := source(sdk)
	if err != nil {
		return err
	}

	if err := setup(url, name); err != nil {
		return err
	}

	return nil
}

// source retrieves the URL for the given SDK.
func source(sdk string) (string, error) {
	url, ok := repos[sdk]
	if !ok {
		return "", fmt.Errorf("unsupported sdk: %s", sdk)
	}

	return url, nil
}

// setup downloads and unzips the SDK to the destination folder.
func setup(url, dest string) error {
	tmp := dest + ".zip"
	if err := download(url, tmp); err != nil {
		return err
	}
	defer os.Remove(tmp)

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
	defer res.Body.Close()

	if err := checkstatus(res); err != nil {
		return err
	}

	out, err := os.Create(file)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, res.Body)
	return err
}

// checkstatus verifies the HTTP response for a successful status.
func checkstatus(res *http.Response) error {
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to fetch template: %s", res.Status)
	}

	return nil
}

// unzip extracts the contents of a zip file to the destination folder.
func unzip(src, dest string) error {
	r, err := zip.OpenReader(src)
	if err != nil {
		return err
	}
	defer r.Close()

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
	defer out.Close()

	rc, err := f.Open()
	if err != nil {
		return err
	}
	defer rc.Close()

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

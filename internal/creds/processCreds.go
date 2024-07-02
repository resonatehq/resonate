package creds

import (
	"log/slog"
)

type Auth struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

func GetProcessedCreds(credList []interface{}) map[string]string {
	authMap := make(map[string]string)
	for _, item := range credList {
		entry, ok := item.(map[string]interface{})
		if !ok {
			slog.Error("Invalid format in 'auth' section")
			continue
		}
		username, usernameOk := entry["username"].(string)
		password, passwordOk := entry["password"].(string)
		if !usernameOk || !passwordOk {
			slog.Error("Invalid username or password format in 'auth' section")
			continue
		}
		authMap[username] = password
	}
	return authMap
}

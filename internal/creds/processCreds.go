package creds

import (
	"errors"
	"log/slog"
)

type Auth struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type CredentialsList struct {
	Users []Auth `yaml:"auth"`
}

var CredsFromFile CredentialsList

func GetCredentials() (CredentialsList, error) {
	if len(CredsFromFile.Users) == 0 {
		slog.Error("CredentialsList is empty", "error", errors.New("Credentials are empty."))
		return CredentialsList{}, errors.New("Credentials are empty.")
	} else {
		return CredsFromFile, nil
	}
}

func GetProcessedCreds(credList *CredentialsList) map[string]string {
	credentials := make(map[string]string)
	for _, item := range credList.Users {
		credentials[item.Username] = item.Password
	}
	return credentials
}

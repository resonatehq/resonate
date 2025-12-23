package client

import (
	"context"
	"net/http"
	"testing"

	v1 "github.com/resonatehq/resonate/pkg/client/v1"
)

func TestBearerTokenInjection(t *testing.T) {
	token := "test-jwt-token"
	option := bearerToken(token)

	mockClient := &v1.Client{
		RequestEditors: []v1.RequestEditorFn{},
	}

	err := option(mockClient)
	if err != nil {
		t.Fatalf("bearerToken() should not return an error, got %v", err)
	}

	if len(mockClient.RequestEditors) != 1 {
		t.Errorf("Expected 1 RequestEditor, got %d", len(mockClient.RequestEditors))
	}

	// Test that the RequestEditor sets the correct Authorization header
	req, _ := http.NewRequest("GET", "http://example.com", nil)
	ctx := context.Background()

	err = mockClient.RequestEditors[0](ctx, req)
	if err != nil {
		t.Fatalf("RequestEditor should not return an error, got %v", err)
	}

	expectedAuth := "Bearer " + token
	if auth := req.Header.Get("Authorization"); auth != expectedAuth {
		t.Errorf("Expected Authorization header %q, got %q", expectedAuth, auth)
	}
}

func TestSetBearerToken(t *testing.T) {
	c := New().(*client)
	token := "my-test-token"

	c.SetBearerToken(token)

	if c.token != token {
		t.Errorf("Expected token %q, got %q", token, c.token)
	}
}

func TestSetBearerTokenWithSetup(t *testing.T) {
	c := New().(*client)
	token := "my-jwt-token"

	c.SetBearerToken(token)

	// Setup will use the token to create the HTTP client
	err := c.Setup("http://localhost:8001")
	if err != nil {
		t.Fatalf("Setup() failed: %v", err)
	}

	if c.v1 == nil {
		t.Error("Expected v1 client to be initialized")
	}
}

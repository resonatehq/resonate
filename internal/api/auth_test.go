package api

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
)

// Helper function to generate RSA key pair for testing
func generateRSAKeyPair() (*rsa.PrivateKey, *rsa.PublicKey, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, err
	}
	return privateKey, &privateKey.PublicKey, nil
}

// Helper function to create a PEM-encoded public key
func createPublicKeyPEM(pubKey *rsa.PublicKey) ([]byte, error) {
	pubKeyBytes, err := x509.MarshalPKIXPublicKey(pubKey)
	if err != nil {
		return nil, err
	}
	pubKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: pubKeyBytes,
	})
	return pubKeyPEM, nil
}

// Helper function to create a valid JWT token
func createToken(privKey *rsa.PrivateKey, claims *Claims) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	return token.SignedString(privKey)
}

// TestNewJWTAuthenticator tests the constructor
func TestNewJWTAuthenticator(t *testing.T) {
	_, pubKey, err := generateRSAKeyPair()
	if err != nil {
		t.Fatalf("Failed to generate RSA key pair: %v", err)
	}

	pubKeyPEM, err := createPublicKeyPEM(pubKey)
	if err != nil {
		t.Fatalf("Failed to create PEM: %v", err)
	}

	t.Run("valid public key", func(t *testing.T) {
		auth, err := NewJWTAuthenticator(pubKeyPEM)
		if err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}
		if auth == nil {
			t.Errorf("Expected authenticator, got nil")
		}
	})

	t.Run("invalid PEM block", func(t *testing.T) {
		_, err := NewJWTAuthenticator([]byte("invalid pem"))
		if err == nil {
			t.Errorf("Expected error for invalid PEM, got nil")
		}
	})

	t.Run("non-RSA public key", func(t *testing.T) {
		// This would require an EC key, but for simplicity we'll test with an invalid key
		invalidPEM := []byte(`-----BEGIN PUBLIC KEY-----
MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBALRiMLAA
-----END PUBLIC KEY-----`)
		_, err := NewJWTAuthenticator(invalidPEM)
		if err == nil {
			t.Errorf("Expected error for invalid key, got nil")
		}
	})
}

// TestAuthenticate tests JWT token validation
func TestAuthenticate(t *testing.T) {
	privKey, pubKey, err := generateRSAKeyPair()
	if err != nil {
		t.Fatalf("Failed to generate RSA key pair: %v", err)
	}

	pubKeyPEM, err := createPublicKeyPEM(pubKey)
	if err != nil {
		t.Fatalf("Failed to create PEM: %v", err)
	}

	auth, err := NewJWTAuthenticator(pubKeyPEM)
	if err != nil {
		t.Fatalf("Failed to create authenticator: %v", err)
	}

	role := "user"
	prefix := "test:"

	t.Run("missing authorization header", func(t *testing.T) {
		req := &t_api.Request{
			Metadata: map[string]string{},
			Payload:  &t_api.EchoRequest{Data: "test"},
		}
		_, err := auth.authenticate(req)
		if err == nil {
			t.Errorf("Expected error for missing header, got nil")
		}
	})

	t.Run("valid token", func(t *testing.T) {
		claims := &Claims{
			Role:   role,
			Prefix: &prefix,
			RegisteredClaims: jwt.RegisteredClaims{
				ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
			},
		}
		token, err := createToken(privKey, claims)
		if err != nil {
			t.Fatalf("Failed to create token: %v", err)
		}

		req := &t_api.Request{
			Metadata: map[string]string{"authorization": token},
			Payload:  &t_api.EchoRequest{Data: "test"},
		}
		returnedClaims, err := auth.authenticate(req)
		if err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}
		if returnedClaims == nil {
			t.Errorf("Expected claims, got nil")
		} else {
			if returnedClaims.Role != role {
				t.Errorf("Expected role %s, got %s", role, returnedClaims.Role)
			}
		}
	})

	t.Run("expired token", func(t *testing.T) {
		claims := &Claims{
			Role:   role,
			Prefix: &prefix,
			RegisteredClaims: jwt.RegisteredClaims{
				ExpiresAt: jwt.NewNumericDate(time.Now().Add(-time.Hour)),
			},
		}
		token, err := createToken(privKey, claims)
		if err != nil {
			t.Fatalf("Failed to create token: %v", err)
		}

		req := &t_api.Request{
			Metadata: map[string]string{"authorization": token},
			Payload:  &t_api.EchoRequest{Data: "test"},
		}
		_, err = auth.authenticate(req)
		if err == nil {
			t.Errorf("Expected error for expired token, got nil")
		}
	})
}

// TestAuthorize tests authorization logic
func TestAuthorize(t *testing.T) {
	t.Run("admin role always authorized", func(t *testing.T) {
		adminClaims := &Claims{
			Role: "admin",
		}
		err := (&JwtAuthenticator{}).authorize(adminClaims, nil)
		if err != nil {
			t.Errorf("Expected no error for admin, got: %v", err)
		}
	})

	t.Run("admin role with different case", func(t *testing.T) {
		for _, role := range []string{"ADMIN", "Admin", "AdMiN"} {
			adminClaims := &Claims{
				Role: role,
			}
			err := (&JwtAuthenticator{}).authorize(adminClaims, nil)
			if err != nil {
				t.Errorf("Expected no error for admin role %s, got: %v", role, err)
			}
		}
	})

	t.Run("non-admin with no prefix", func(t *testing.T) {
		claims := &Claims{
			Role:   "user",
			Prefix: nil,
		}
		err := (&JwtAuthenticator{}).authorize(claims, nil)
		if err == nil {
			t.Errorf("Expected error for non-admin with no prefix, got nil")
		}
	})

	t.Run("non-admin with empty prefix", func(t *testing.T) {
		emptyPrefix := ""
		claims := &Claims{
			Role:   "user",
			Prefix: &emptyPrefix,
		}
		err := (&JwtAuthenticator{}).authorize(claims, nil)
		if err != nil {
			t.Errorf("Expected no error for empty prefix, got: %v", err)
		}
	})

	t.Run("nil claims", func(t *testing.T) {
		err := (&JwtAuthenticator{}).authorize(nil, nil)
		if err == nil {
			t.Errorf("Expected error for nil claims, got nil")
		}
	})
}

// TestMatchPromisePrefix tests prefix matching for all request types
func TestMatchPromisePrefix(t *testing.T) {
	tests := []struct {
		name        string
		payload     t_api.RequestPayload
		prefix      string
		shouldMatch bool
		shouldPanic bool
	}{
		// Promise requests
		{
			name:        "ReadPromiseRequest - matching prefix",
			payload:     &t_api.ReadPromiseRequest{Id: "test.123"},
			prefix:      "test",
			shouldMatch: true,
		},
		{
			name:        "ReadPromiseRequest - non-matching prefix",
			payload:     &t_api.ReadPromiseRequest{Id: "other.123"},
			prefix:      "test",
			shouldMatch: false,
		},
		{
			name:        "CreatePromiseRequest - matching prefix",
			payload:     &t_api.CreatePromiseRequest{Id: "app.promise.1"},
			prefix:      "app",
			shouldMatch: true,
		},
		{
			name:        "CompletePromiseRequest - matching prefix",
			payload:     &t_api.CompletePromiseRequest{Id: "my-app.promise.1"},
			prefix:      "my-app",
			shouldMatch: true,
		},
		{
			name:        "CreatePromiseAndTaskRequest - matching prefix",
			payload:     &t_api.CreatePromiseAndTaskRequest{Promise: &t_api.CreatePromiseRequest{Id: "prefix.id"}},
			prefix:      "prefix",
			shouldMatch: true,
		},
		// Callback requests
		{
			name:        "CreateCallbackRequest - matching prefix",
			payload:     &t_api.CreateCallbackRequest{PromiseId: "test.123"},
			prefix:      "test",
			shouldMatch: true,
		},
		// Schedule requests
		{
			name:        "ReadScheduleRequest - matching prefix",
			payload:     &t_api.ReadScheduleRequest{Id: "schedule.1"},
			prefix:      "schedule",
			shouldMatch: true,
		},
		{
			name:        "CreateScheduleRequest - matching prefix",
			payload:     &t_api.CreateScheduleRequest{PromiseId: "sched.promise.1"},
			prefix:      "sched",
			shouldMatch: true,
		},
		{
			name:        "DeleteScheduleRequest - matching prefix",
			payload:     &t_api.DeleteScheduleRequest{Id: "del.sched.1"},
			prefix:      "del",
			shouldMatch: true,
		},
		{
			name:        "SearchSchedulesRequest - matching prefix",
			payload:     &t_api.SearchSchedulesRequest{Id: "search.1"},
			prefix:      "search",
			shouldMatch: true,
		},
		// Task requests with special ID formats
		{
			name:        "ClaimTaskRequest - resume format",
			payload:     &t_api.ClaimTaskRequest{Id: "__resume:test.promise:another"},
			prefix:      "test",
			shouldMatch: true,
		},
		{
			name:        "ClaimTaskRequest - invoke format",
			payload:     &t_api.ClaimTaskRequest{Id: "__invoke:test.root:test.promise"},
			prefix:      "test",
			shouldMatch: true,
		},
		{
			name:        "ClaimTaskRequest - notify format",
			payload:     &t_api.ClaimTaskRequest{Id: "__notify:test.promise:id.extra"},
			prefix:      "test.",
			shouldMatch: true,
		},
		{
			name:        "CompleteTaskRequest - non-matching prefix",
			payload:     &t_api.CompleteTaskRequest{Id: "__resume:other.promise:child.promise"},
			prefix:      "test",
			shouldMatch: false,
		},
		// Special cases - no prefix check
		{
			name:        "HeartbeatTasksRequest - always authorized",
			payload:     &t_api.HeartbeatTasksRequest{ProcessId: "any:id"},
			prefix:      "test:",
			shouldMatch: true,
		},
		{
			name:        "EchoRequest - always authorized",
			payload:     &t_api.EchoRequest{Data: "test"},
			prefix:      "test:",
			shouldMatch: true,
		},
		{
			name:        "SearchPromisesRequest - matching prefix",
			payload:     &t_api.SearchPromisesRequest{Id: "search:promise"},
			prefix:      "search:",
			shouldMatch: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &t_api.Request{Payload: tt.payload}
			err := matchPromisePrefix(req, tt.prefix)
			if tt.shouldMatch && err != nil {
				t.Errorf("Expected match, got error: %v", err)
			}
			if !tt.shouldMatch && err == nil {
				t.Errorf("Expected no match (error), got nil")
			}
		})
	}
}

// TestProcess tests the full authentication and authorization flow
func TestProcess(t *testing.T) {
	privKey, pubKey, err := generateRSAKeyPair()
	if err != nil {
		t.Fatalf("Failed to generate RSA key pair: %v", err)
	}

	pubKeyPEM, err := createPublicKeyPEM(pubKey)
	if err != nil {
		t.Fatalf("Failed to create PEM: %v", err)
	}

	auth, err := NewJWTAuthenticator(pubKeyPEM)
	if err != nil {
		t.Fatalf("Failed to create authenticator: %v", err)
	}

	prefix := "app"

	t.Run("full flow - authorized user", func(t *testing.T) {
		claims := &Claims{
			Role:   "user",
			Prefix: &prefix,
			RegisteredClaims: jwt.RegisteredClaims{
				ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
			},
		}
		token, err := createToken(privKey, claims)
		if err != nil {
			t.Fatalf("Failed to create token: %v", err)
		}

		req := &t_api.Request{
			Metadata: map[string]string{"authorization": token},
			Payload:  &t_api.ReadPromiseRequest{Id: "app.promise.1"},
		}
		apiErr := auth.Process(req)
		if apiErr != nil {
			t.Errorf("Expected no error for authorized request, got: %v", apiErr)
		}
	})

	t.Run("full flow - admin user", func(t *testing.T) {
		claims := &Claims{
			Role: "admin",
			RegisteredClaims: jwt.RegisteredClaims{
				ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
			},
		}
		token, err := createToken(privKey, claims)
		if err != nil {
			t.Fatalf("Failed to create token: %v", err)
		}

		req := &t_api.Request{
			Metadata: map[string]string{"authorization": token},
			Payload:  &t_api.ReadPromiseRequest{Id: "any.prefix.promise"},
		}
		apiErr := auth.Process(req)
		if apiErr != nil {
			t.Errorf("Expected no error for admin request, got: %v", apiErr)
		}
	})

	t.Run("full flow - unauthorized prefix", func(t *testing.T) {
		claims := &Claims{
			Role:   "user",
			Prefix: &prefix,
			RegisteredClaims: jwt.RegisteredClaims{
				ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
			},
		}
		token, err := createToken(privKey, claims)
		if err != nil {
			t.Fatalf("Failed to create token: %v", err)
		}

		req := &t_api.Request{
			Metadata: map[string]string{"authorization": token},
			Payload:  &t_api.ReadPromiseRequest{Id: "other.promise.1"},
		}
		apiErr := auth.Process(req)
		if apiErr == nil {
			t.Errorf("Expected error for unauthorized prefix, got nil")
		}
	})

	t.Run("full flow - missing token", func(t *testing.T) {
		req := &t_api.Request{
			Metadata: map[string]string{},
			Payload:  &t_api.ReadPromiseRequest{Id: "app:promise:1"},
		}
		apiErr := auth.Process(req)
		if apiErr == nil {
			t.Errorf("Expected error for missing token, got nil")
		}
	})
}

// TestProcessMultipleRequestTypes tests authorization for all request types
func TestProcessMultipleRequestTypes(t *testing.T) {
	privKey, pubKey, err := generateRSAKeyPair()
	if err != nil {
		t.Fatalf("Failed to generate RSA key pair: %v", err)
	}

	pubKeyPEM, err := createPublicKeyPEM(pubKey)
	if err != nil {
		t.Fatalf("Failed to create PEM: %v", err)
	}

	auth, err := NewJWTAuthenticator(pubKeyPEM)
	if err != nil {
		t.Fatalf("Failed to create authenticator: %v", err)
	}

	prefix := "test:"
	claims := &Claims{
		Role:   "user",
		Prefix: &prefix,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
	}
	token, err := createToken(privKey, claims)
	if err != nil {
		t.Fatalf("Failed to create token: %v", err)
	}

	metadata := map[string]string{"authorization": token}

	tests := []struct {
		name       string
		payload    t_api.RequestPayload
		shouldPass bool
	}{
		{
			name:       "ReadPromiseRequest - authorized",
			payload:    &t_api.ReadPromiseRequest{Id: "test:promise"},
			shouldPass: true,
		},
		{
			name:       "CreatePromiseRequest - authorized",
			payload:    &t_api.CreatePromiseRequest{Id: "test:promise"},
			shouldPass: true,
		},
		{
			name:       "CompletePromiseRequest - authorized",
			payload:    &t_api.CompletePromiseRequest{Id: "test:promise"},
			shouldPass: true,
		},
		{
			name:       "CreateCallbackRequest - authorized",
			payload:    &t_api.CreateCallbackRequest{PromiseId: "test:promise"},
			shouldPass: true,
		},
		{
			name:       "EchoRequest - always authorized",
			payload:    &t_api.EchoRequest{Data: "test"},
			shouldPass: true,
		},
		{
			name:       "ReadPromiseRequest - unauthorized",
			payload:    &t_api.ReadPromiseRequest{Id: "other:promise"},
			shouldPass: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &t_api.Request{
				Metadata: metadata,
				Payload:  tt.payload,
			}
			apiErr := auth.Process(req)
			if tt.shouldPass && apiErr != nil {
				t.Errorf("Expected no error, got: %v", apiErr)
			}
			if !tt.shouldPass && apiErr == nil {
				t.Errorf("Expected error, got nil")
			}
		})
	}
}

package api

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"strings"

	"github.com/golang-jwt/jwt/v5"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
)

type Authenticator interface {
	Authenticate(request t_api.Request) (any, error)
	Authorize(claims any, request t_api.Request) error
}

type NoopAuthenticator struct{}

func (a *NoopAuthenticator) Authenticate(t_api.Request) (any, error) {
	return nil, nil
}

func (a *NoopAuthenticator) Authorize(any, t_api.Request) error {
	return nil
}

type JwtAuthenticator struct {
	publicKey *rsa.PublicKey
}

func NewJWTAuthenticator(publicKeyPEM []byte) (*JwtAuthenticator, error) {
	block, _ := pem.Decode(publicKeyPEM)
	if block == nil {
		return nil, fmt.Errorf("failed to decode PEM block")
	}

	publicKey, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse public key: %w", err)
	}

	rsaPublicKey, ok := publicKey.(*rsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("not an RSA public key")
	}

	return &JwtAuthenticator{
		publicKey: rsaPublicKey,
	}, nil
}

type Claims struct {
	PromisePrefix string `json:"promisePrefix"`
	Role          string `json:"role"`
	jwt.RegisteredClaims
}

func (a *JwtAuthenticator) Authenticate(req t_api.Request) (any, error) {
	authHeader, ok := req.Metadata["Authorization"]
	if !ok {
		return nil, fmt.Errorf("missing authorization header")
	}

	// Extract token from "Bearer <token>" format
	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
		return nil, fmt.Errorf("invalid authorization header format")
	}

	tokenString := parts[1]

	// ParseWithClaims also checks registered claims like expiry time
	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		// Only support RSA (private/public key) signed method
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return a.publicKey, nil
	})

	if err != nil {
		return nil, fmt.Errorf("token validation failed: %w", err)
	}

	if !token.Valid {
		return nil, fmt.Errorf("invalid token")
	}

	claims, ok := token.Claims.(*Claims)
	if !ok {
		return nil, fmt.Errorf("invalid token claims")
	}

	return claims, nil
}

func (a *JwtAuthenticator) Authorize(claims any, req t_api.Request) error {
	// TODO(avillega): empty prefix should match everything, consider a
	// DSL that uses '*' as a wildcard
	// TODO(avillega): support a list of prefixes instead of just a single
	// prefix. 
	c, ok := claims.(*Claims)
	if !ok {
		return fmt.Errorf("invalid claims type")
	}

	// Admins have access to all promise prefixes
	if strings.ToLower(c.Role) == "admin" {
		return nil
	}

	if c.PromisePrefix == "" {
		return fmt.Errorf("not matched authorized prefix")
	}

	return matchPromisePrefix(req, c.PromisePrefix)
}

func matchPromisePrefix(req t_api.Request, prefix string) error {
	var id string
	switch r := req.Payload.(type) {
	case *t_api.ReadPromiseRequest:
		id = r.Id
	case *t_api.CreatePromiseRequest:
		id = r.Id
	case *t_api.CreatePromiseAndTaskRequest:
		id = r.Promise.Id
	case *t_api.CompletePromiseRequest:
		id = r.Id
	case *t_api.ClaimTaskRequest:
		id = extractPromiseId(r.Id)
	case *t_api.CompleteTaskRequest:
		id = extractPromiseId(r.Id)
	case *t_api.DropTaskRequest:
		id = extractPromiseId(r.Id)
	case *t_api.CreateCallbackRequest:
		id = r.PromiseId
	case *t_api.ReadScheduleRequest:
		id = r.Id
	case *t_api.CreateScheduleRequest:
		id = r.PromiseId
	case *t_api.DeleteScheduleRequest:
		id = r.Id
	case *t_api.AcquireLockRequest:
		id = r.ResourceId
	case *t_api.ReleaseLockRequest:
		id = r.ResourceId
	case *t_api.SearchPromisesRequest:
		id = r.Id
	case *t_api.SearchSchedulesRequest:
		id = r.Id
	case *t_api.HeartbeatLocksRequest:
		return nil
	case *t_api.HeartbeatTasksRequest:
		return nil
	case *t_api.EchoRequest:
		return nil
	default:
		panic("unreachable: unexpected request type")
	}

	if strings.HasPrefix(id, prefix) {
		return nil
	}

	return fmt.Errorf("unauthorized prefix")

}

func extractPromiseId(taskId string) string {
	// We expect the taskId to have the one of following formats
	// __resume:{promiseId}:{another}
	// __notify:{promiseId}:{another}
	// __invoke:{promiseId}
	// if that changes we need to change this code
	if !strings.HasPrefix(taskId, "__resume") &&
		!strings.HasPrefix(taskId, "__invoke") &&
		!strings.HasPrefix(taskId, "__notify") {
		panic("taskId must start with __resume, __invoke, or __notify")
	}
	start := strings.Index(taskId, ":")
	util.Assert(start != -1, "taskId prefix must be separated by ':'")
	return taskId[start+1:]
}

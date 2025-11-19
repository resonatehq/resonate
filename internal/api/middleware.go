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

type Middleware interface {
	Process(r *t_api.Request) *t_api.Error
}

// TODO(avillega): This authenticator is the first middleware that we currently have
// as we have more middlewares we should rethink where they to put them
type JwtAuthenticator struct {
	publicKey *rsa.PublicKey
}

type Claims struct {
	Prefix *string `json:"prefix"`
	Role   string  `json:"role"`
	jwt.RegisteredClaims
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

func (a *JwtAuthenticator) Process(req *t_api.Request) *t_api.Error {
	claims, err := a.authenticate(req)
	if err != nil {
		return t_api.NewError(t_api.StatusUnauthorized, err)
	}

	err = a.authorize(claims, req)
	if err != nil {
		return t_api.NewError(t_api.StatusForbidden, err)
	}

	return nil
}

func (a *JwtAuthenticator) authenticate(req *t_api.Request) (*Claims, error) {
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

	claims := &Claims{}

	// ParseWithClaims also checks registered claims like expiry time
	token, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (interface{}, error) {
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

	return claims, nil
}

func (a *JwtAuthenticator) authorize(claims *Claims, req *t_api.Request) error {
	if claims == nil {
		return fmt.Errorf("found no claims")
	}
	// Admins have access to all promise prefixes
	if strings.ToLower(claims.Role) == "admin" {
		return nil
	}

	if claims.Prefix == nil {
		return fmt.Errorf("unauthorized prefix")
	}

	if *claims.Prefix == "" {
		return nil
	}

	return matchPromisePrefix(req, *claims.Prefix)
}

func matchPromisePrefix(req *t_api.Request, prefix string) error {
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

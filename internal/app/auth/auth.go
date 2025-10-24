package auth

import (
	"errors"
	"fmt"
	"maps"
	"net/http"
	"os"
	"slices"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

// ContextIdentityKey is used to store authentication details in request contexts.
const ContextIdentityKey = "auth.identity"

// Identity represents an authenticated caller.
type Identity struct {
	Subject string
	Claims  jwt.MapClaims
	Token   *jwt.Token
}

// Error represents an authentication failure response.
type Error struct {
	Status  int
	Message string
	Headers map[string]string
	Err     error
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Message != "" {
		return e.Message
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	return fmt.Sprintf("authentication failed with status %d", e.Status)
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// Write renders the error as an HTTP response.
func (e *Error) Write(w http.ResponseWriter) {
	if e == nil {
		return
	}
	if len(e.Headers) > 0 {
		keys := maps.Keys(e.Headers)
		slices.Sort(keys)
		for _, key := range keys {
			w.Header().Set(key, e.Headers[key])
		}
	}
	http.Error(w, e.Error(), e.Status)
}

// Authenticator validates inbound requests.
type Authenticator interface {
	Enabled() bool
	Authenticate(r *http.Request) (*Identity, *Error)
}

// New creates an Authenticator from configuration. It returns nil when
// authentication is disabled.
func New(cfg *Config) (Authenticator, error) {
	if cfg == nil {
		return nil, nil
	}

	provider := strings.TrimSpace(strings.ToLower(cfg.Provider))
	if provider == "" && len(cfg.Basic) > 0 {
		provider = "basic"
	}

	switch provider {
	case "":
		return nil, nil
	case "basic":
		if len(cfg.Basic) == 0 {
			return nil, errors.New("basic auth provider requires credentials")
		}
		return newBasicAuthenticator(cfg.Basic), nil
	case "jwt":
		return newJWTAuthenticator(&cfg.JWT)
	default:
		return nil, fmt.Errorf("unsupported auth provider %q", cfg.Provider)
	}
}

// GinMiddleware creates a gin middleware that enforces authentication.
func GinMiddleware(a Authenticator) gin.HandlerFunc {
	if a == nil || !a.Enabled() {
		return nil
	}

	return func(c *gin.Context) {
		identity, err := a.Authenticate(c.Request)
		if err != nil {
			err.Write(c.Writer)
			c.Abort()
			return
		}
		if identity != nil {
			c.Set(ContextIdentityKey, identity)
		}
		c.Next()
	}
}

// RequireHTTP enforces authentication for a standard net/http handler. It returns
// the authenticated identity (when available) and a boolean indicating whether
// the request should proceed.
func RequireHTTP(a Authenticator, w http.ResponseWriter, r *http.Request) (*Identity, bool) {
	if a == nil || !a.Enabled() {
		return nil, true
	}

	identity, err := a.Authenticate(r)
	if err != nil {
		err.Write(w)
		return nil, false
	}

	return identity, true
}

func loadKeyMaterial(cfg *JWTConfig) ([]byte, error) {
	if cfg.KeyFile != "" {
		data, err := os.ReadFile(cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("read jwt key file: %w", err)
		}
		return data, nil
	}
	if cfg.Key != "" {
		return []byte(cfg.Key), nil
	}
	return nil, errors.New("jwt key or key-file must be provided")
}

func signingKey(algorithm string, material []byte) (any, error) {
	switch algorithm {
	case jwt.SigningMethodHS256.Alg(), jwt.SigningMethodHS384.Alg(), jwt.SigningMethodHS512.Alg():
		return material, nil
	case jwt.SigningMethodRS256.Alg(), jwt.SigningMethodRS384.Alg(), jwt.SigningMethodRS512.Alg():
		return jwt.ParseRSAPublicKeyFromPEM(material)
	case jwt.SigningMethodES256.Alg(), jwt.SigningMethodES384.Alg(), jwt.SigningMethodES512.Alg():
		return jwt.ParseECPublicKeyFromPEM(material)
	case jwt.SigningMethodEdDSA.Alg():
		return jwt.ParseEdPublicKeyFromPEM(material)
	default:
		return nil, fmt.Errorf("unsupported jwt algorithm %q", algorithm)
	}
}

func parserOptions(cfg *JWTConfig) []jwt.ParserOption {
	opts := []jwt.ParserOption{
		jwt.WithValidMethods([]string{cfg.Algorithm}),
	}
	if cfg.ClockSkew > 0 {
		opts = append(opts, jwt.WithLeeway(cfg.ClockSkew))
	}
	if cfg.Issuer != "" {
		opts = append(opts, jwt.WithIssuer(cfg.Issuer))
	}
	if len(cfg.Audience) > 0 {
		opts = append(opts, jwt.WithAudience(cfg.Audience...))
	}
	return opts
}

func bearerUnauthorized(message string, cause error) *Error {
	return &Error{
		Status:  http.StatusUnauthorized,
		Message: message,
		Headers: map[string]string{"WWW-Authenticate": "Bearer"},
		Err:     cause,
	}
}

func basicUnauthorized(cause error) *Error {
	return &Error{
		Status:  http.StatusUnauthorized,
		Message: "unauthorized",
		Headers: map[string]string{"WWW-Authenticate": `Basic realm="Restricted"`},
		Err:     cause,
	}
}

func newBasicAuthenticator(credentials map[string]string) Authenticator {
	sanitized := map[string]string{}
	if len(credentials) > 0 {
		users := maps.Keys(credentials)
		slices.Sort(users)
		for _, user := range users {
			pass := credentials[user]
			trimmed := strings.TrimSpace(user)
			if trimmed == "" {
				continue
			}
			sanitized[trimmed] = pass
		}
	}
	return &basicAuthenticator{credentials: sanitized}
}

func newJWTAuthenticator(cfg *JWTConfig) (Authenticator, error) {
	algorithm := cfg.Algorithm
	if algorithm == "" {
		algorithm = jwt.SigningMethodHS256.Name
	}
	method := jwt.GetSigningMethod(algorithm)
	if method == nil {
		return nil, fmt.Errorf("unknown jwt signing algorithm %q", algorithm)
	}
	cfg.Algorithm = method.Alg()

	material, err := loadKeyMaterial(cfg)
	if err != nil {
		return nil, err
	}
	key, err := signingKey(cfg.Algorithm, material)
	if err != nil {
		return nil, err
	}

	parser := jwt.NewParser(parserOptions(cfg)...)

	return &jwtAuthenticator{
		key:    key,
		parser: parser,
	}, nil
}

type basicAuthenticator struct {
	credentials map[string]string
}

func (a *basicAuthenticator) Enabled() bool {
	return len(a.credentials) > 0
}

func (a *basicAuthenticator) Authenticate(r *http.Request) (*Identity, *Error) {
	username, password, ok := r.BasicAuth()
	if !ok {
		return nil, basicUnauthorized(errors.New("missing basic auth header"))
	}

	expected, exists := a.credentials[username]
	if !exists || expected != password {
		return nil, basicUnauthorized(errors.New("invalid credentials"))
	}

	return &Identity{Subject: username}, nil
}

type jwtAuthenticator struct {
	key    any
	parser *jwt.Parser
}

func (a *jwtAuthenticator) Enabled() bool { return true }

func (a *jwtAuthenticator) Authenticate(r *http.Request) (*Identity, *Error) {
	header := r.Header.Get("Authorization")
	if header == "" {
		return nil, bearerUnauthorized("missing authorization header", errors.New("missing authorization header"))
	}

	parts := strings.SplitN(header, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return nil, bearerUnauthorized("invalid authorization header", errors.New("expected bearer token"))
	}

	token, err := a.parser.ParseWithClaims(parts[1], jwt.MapClaims{}, func(*jwt.Token) (any, error) {
		return a.key, nil
	})
	if err != nil {
		return nil, bearerUnauthorized("invalid token", err)
	}
	if !token.Valid {
		return nil, bearerUnauthorized("invalid token", errors.New("token validation failed"))
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, bearerUnauthorized("invalid token", errors.New("token claims unexpected"))
	}

	subject, _ := claims["sub"].(string)

	return &Identity{Subject: subject, Claims: claims, Token: token}, nil
}

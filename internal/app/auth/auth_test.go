package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
)

func TestNewBasicAuthenticator(t *testing.T) {
	t.Parallel()

	a, err := New(&Config{Provider: "basic", Basic: map[string]string{"user": "pass"}})
	require.NoError(t, err)
	require.NotNil(t, a)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.SetBasicAuth("user", "pass")
	identity, authErr := a.Authenticate(req)
	require.Nil(t, authErr)
	require.NotNil(t, identity)
	require.Equal(t, "user", identity.Subject)

	badReq := httptest.NewRequest(http.MethodGet, "/", nil)
	badReq.SetBasicAuth("user", "wrong")
	_, authErr = a.Authenticate(badReq)
	require.NotNil(t, authErr)
	require.Equal(t, http.StatusUnauthorized, authErr.Status)
}

func TestNewBasicAuthenticatorRequiresCredentials(t *testing.T) {
	t.Parallel()

	a, err := New(&Config{Provider: "basic"})
	require.Error(t, err)
	require.Nil(t, a)
}

func TestJWTAuthenticator(t *testing.T) {
	t.Parallel()

	cfg := &Config{Provider: "jwt", JWT: JWTConfig{Algorithm: "HS256", Issuer: "resonate", Audience: []string{"resonate"}, Key: "secret", ClockSkew: time.Second}}
	a, err := New(cfg)
	require.NoError(t, err)
	require.NotNil(t, a)

	token := issueTestToken(t, cfg.JWT, "user")

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	identity, authErr := a.Authenticate(req)
	require.Nil(t, authErr)
	require.NotNil(t, identity)
	require.Equal(t, "user", identity.Subject)

	missing := httptest.NewRequest(http.MethodGet, "/", nil)
	_, authErr = a.Authenticate(missing)
	require.NotNil(t, authErr)
	require.Equal(t, http.StatusUnauthorized, authErr.Status)
}

func TestGinMiddlewareAndRequireHTTP(t *testing.T) {
	t.Parallel()

	a, err := New(&Config{Provider: "basic", Basic: map[string]string{"user": "pass"}})
	require.NoError(t, err)

	// Gin middleware should block unauthorized requests
	gin.SetMode(gin.TestMode)
	router := gin.New()
	middleware := GinMiddleware(a)
	require.NotNil(t, middleware)

	router.Use(middleware)
	router.GET("/", func(c *gin.Context) { c.Status(http.StatusOK) })

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusUnauthorized, w.Code)

	// RequireHTTP helper should permit authorized requests
	resp := httptest.NewRecorder()
	authedReq := httptest.NewRequest(http.MethodGet, "/", nil)
	authedReq.SetBasicAuth("user", "pass")
	_, ok := RequireHTTP(a, resp, authedReq)
	require.True(t, ok)
	require.Equal(t, http.StatusOK, resp.Code)
}

func issueTestToken(t *testing.T, cfg JWTConfig, subject string) string {
	t.Helper()

	claims := jwt.RegisteredClaims{
		Subject:   subject,
		Issuer:    cfg.Issuer,
		Audience:  jwt.ClaimStrings(cfg.Audience),
		IssuedAt:  jwt.NewNumericDate(time.Now()),
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute)),
	}

	method := jwt.SigningMethodHS256
	token := jwt.NewWithClaims(method, claims)
	signed, err := token.SignedString([]byte(cfg.Key))
	require.NoError(t, err)

	return signed
}

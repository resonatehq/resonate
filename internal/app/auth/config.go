package auth

import "time"

// Config defines pluggable authentication settings.
type Config struct {
	// Provider selects which authentication strategy to enable. Supported values
	// are "basic" and "jwt". When empty authentication is disabled unless basic
	// credentials are provided via the Basic field.
	Provider string `flag:"provider" desc:"auth provider to use (basic,jwt)"`
	// Basic maintains backwards compatibility with the legacy map based basic
	// authentication configuration. When populated the provider defaults to
	// "basic".
	Basic map[string]string `flag:"-" desc:"http basic auth username password pairs"`
	// JWT holds JSON web token validation options.
	JWT JWTConfig `flag:"jwt" desc:"jwt auth settings"`
}

// JWTConfig represents the configuration required to validate JWT bearer tokens.
type JWTConfig struct {
	Algorithm string        `flag:"algorithm" desc:"jwt signing algorithm" default:"HS256"`
	Audience  []string      `flag:"audience" desc:"expected jwt audiences"`
	Issuer    string        `flag:"issuer" desc:"expected jwt issuer"`
	Key       string        `flag:"key" desc:"jwt verification key or shared secret"`
	KeyFile   string        `flag:"key-file" desc:"path to jwt verification key or shared secret"`
	ClockSkew time.Duration `flag:"clock-skew" desc:"clock skew tolerance when validating tokens" default:"30s"`
}

# Dev Auth Keys

These are **intentionally public** Ed25519 keys for local development and testing only.

- `private.pem` — used by test clients to sign JWTs
- `public.pem` — mounted into the resonate server to verify JWTs

**Do not use these keys in production.**

## Regenerating the keys

```sh
openssl genpkey -algorithm ed25519 -out config/auth/dev/private.pem
openssl pkey -in config/auth/dev/private.pem -pubout -out config/auth/dev/public.pem
```

## Generating JWT tokens

### Install

```sh
brew install jwt-cli
```

### Generate a token (admin role, 1 hour expiry)

```sh
jwt encode \
  --alg EDDSA \
  --secret @config/auth/dev/private.pem \
  --exp=1h \
  --payload role=admin
```

### Generate a token with a prefix claim

```sh
jwt encode \
  --alg EDDSA \
  --secret @config/auth/dev/private.pem \
  --exp=1h \
  --payload role=admin \
  --payload prefix=my-prefix
```

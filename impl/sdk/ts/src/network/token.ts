// =============================================================================
// TokenProvider — pluggable auth token source
// =============================================================================
//
// HttpNetwork and PollMessageSource call `getToken()` to obtain an
// `Authorization: Bearer` token. Implementations can be static (fixed string),
// noop (no auth), or dynamic (e.g. Google OIDC ID tokens that auto-refresh).
//
// TokenProvider takes precedence over the legacy `token` string parameter.
// When both `tokenProvider` and `token` are supplied, `tokenProvider` wins.

export interface TokenProvider {
  /** Returns a bearer token (without the "Bearer " prefix), or undefined for no auth. */
  getToken(): Promise<string | undefined>;
}

// =============================================================================
// StaticTokenProvider — wraps a fixed token string
// =============================================================================

export class StaticTokenProvider implements TokenProvider {
  constructor(private readonly token: string) {}

  getToken(): Promise<string> {
    return Promise.resolve(this.token);
  }
}

// =============================================================================
// NoopTokenProvider — no authentication
// =============================================================================

export class NoopTokenProvider implements TokenProvider {
  getToken(): Promise<undefined> {
    return Promise.resolve(undefined);
  }
}

// =============================================================================
// Helpers
// =============================================================================

/**
 * Resolves a `TokenProvider` from the new `tokenProvider` option and the legacy
 * `token` string. Precedence: `tokenProvider` > `token` > `NoopTokenProvider`.
 */
export function resolveTokenProvider(tokenProvider?: TokenProvider, token?: string): TokenProvider {
  if (tokenProvider) return tokenProvider;
  if (token !== undefined) return new StaticTokenProvider(token);
  return new NoopTokenProvider();
}

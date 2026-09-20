import { GoogleAuth, type IdTokenClient } from "google-auth-library";

export type AuthMode = "auto" | "none" | "bearer" | "oidcIdToken";

export type AuthOptions =
  | { mode?: "auto" }
  | { mode: "none" }
  | { mode: "bearer"; token?: string }
  | { mode: "oidcIdToken"; audience?: string };

export interface ResolvedAuth {
  /** Extra headers to merge into the outbound request (e.g. Authorization). */
  headers: Record<string, string>;
  /** Bearer token passed through to HttpNetwork (for `bearer` mode). */
  token?: string;
}

// Module-level GoogleAuth singleton — handles ADC credential loading, caches the
// underlying credential client, and is safe to share across warm invocations.
const _googleAuth = new GoogleAuth();

// IdTokenClient cache keyed by audience.  Each client manages its own token
// lifecycle (refresh before expiry), so we reuse the same instance per audience.
const _idTokenClients = new Map<string, IdTokenClient>();

async function _idTokenClient(audience: string): Promise<IdTokenClient> {
  let client = _idTokenClients.get(audience);
  if (!client) {
    client = await _googleAuth.getIdTokenClient(audience);
    _idTokenClients.set(audience, client);
  }
  return client;
}

function _headersToRecord(h: Headers): Record<string, string> {
  const out: Record<string, string> = {};
  h.forEach((value, key) => {
    out[key] = value;
  });
  return out;
}

async function _mintIdTokenHeaders(audience: string): Promise<Record<string, string>> {
  const client = await _idTokenClient(audience);
  const h = await client.getRequestHeaders();
  return _headersToRecord(h);
}

/**
 * Resolves outbound auth for a call to the Resonate server at `serverUrl`.
 *
 * `serverUrl` may be undefined when the caller omits it (HttpNetwork falls back
 * to the RESONATE_URL env var in that case).  We mirror that fallback for auth
 * resolution so the auto-HTTPS detection still works.
 *
 * Mode behaviour:
 * - `auto`        — mint an OIDC ID token for HTTPS targets; no auth for HTTP.
 * - `none`        — no auth header.
 * - `bearer`      — static bearer token (options.token, then RESONATE_TOKEN env).
 * - `oidcIdToken` — always mint an OIDC ID token for `audience ?? serverUrl`.
 */
export async function resolveAuth(serverUrl: string | undefined, options: AuthOptions = {}): Promise<ResolvedAuth> {
  // Mirror HttpNetwork's RESONATE_URL fallback so auto-HTTPS detection works
  // even when serverUrl is omitted from the message head.
  const effectiveUrl = serverUrl ?? process.env.RESONATE_URL ?? "";
  const mode = options.mode ?? "auto";

  switch (mode) {
    case "none":
      return { headers: {} };

    case "bearer": {
      const tok = (options as { mode: "bearer"; token?: string }).token;
      // Pass token (possibly undefined) to HttpNetwork; it falls back to
      // RESONATE_TOKEN env var internally when token is undefined.
      return { headers: {}, token: tok };
    }

    case "oidcIdToken": {
      const audience = (options as { mode: "oidcIdToken"; audience?: string }).audience ?? effectiveUrl;
      return { headers: await _mintIdTokenHeaders(audience) };
    }

    case "auto":
      if (effectiveUrl.startsWith("https://")) {
        return { headers: await _mintIdTokenHeaders(effectiveUrl) };
      }
      return { headers: {} };

    default:
      mode satisfies never;
      throw new Error(`unknown mode ${mode}`);
  }
}

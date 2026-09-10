import { browser } from '$app/environment';

/**
 * What the operator can change, and where it is kept.
 *
 * `localStorage`, per browser. Nothing here is sent anywhere except the server
 * the console is already talking to, and a token typed here is stored exactly
 * as typed — this console has no session of its own.
 */
export interface Settings {
  /**
   * The Resonate server to read from. Empty means "the one that served this
   * page", which is the embedded case and the default.
   *
   * A non-empty value must be a server that also serves the console, because
   * `ui.*` is answered on the console's route and nowhere else. Pointing at
   * another origin also needs that server's CORS to allow this one.
   */
  serverUrl: string;
  /** Sent as `head.auth` on every request. Empty means none. */
  token: string;
  /** How often the list screens re-read page one, in milliseconds. */
  pollMs: number;
}

const KEY = 'resonate.console.settings';

export const DEFAULTS: Settings = {
  serverUrl: '',
  token: '',
  // 5s, page one only — the cadence `resonate-ui` settled on.
  pollMs: 5000
};

function load(): Settings {
  if (!browser) return { ...DEFAULTS };
  try {
    const raw = localStorage.getItem(KEY);
    if (!raw) return { ...DEFAULTS };
    const parsed = JSON.parse(raw) as Partial<Settings>;
    return {
      serverUrl: typeof parsed.serverUrl === 'string' ? parsed.serverUrl : DEFAULTS.serverUrl,
      token: typeof parsed.token === 'string' ? parsed.token : DEFAULTS.token,
      pollMs:
        typeof parsed.pollMs === 'number' && parsed.pollMs >= 1000 ? parsed.pollMs : DEFAULTS.pollMs
    };
  } catch {
    return { ...DEFAULTS };
  }
}

/** The live settings. Mutating a field persists it. */
export const settings = $state<Settings>(load());

export function save(next: Settings) {
  settings.serverUrl = next.serverUrl.trim().replace(/\/+$/, '');
  settings.token = next.token.trim();
  settings.pollMs = next.pollMs;
  if (!browser) return;
  try {
    localStorage.setItem(KEY, JSON.stringify({ ...settings }));
  } catch {
    // A browser that refuses storage still runs the console; the settings just
    // do not survive the tab.
  }
}

export function reset() {
  save({ ...DEFAULTS });
}

/** `2s`, `500ms`, `1m` — the spelling the settings field takes and shows. */
export function parseInterval(text: string): number | null {
  const m = /^\s*(\d+(?:\.\d+)?)\s*(ms|s|m)?\s*$/.exec(text);
  if (!m) return null;
  const n = Number(m[1]);
  const unit = m[2] ?? 's';
  const ms = unit === 'ms' ? n : unit === 's' ? n * 1000 : n * 60_000;
  if (!Number.isFinite(ms) || ms < 1000 || ms > 600_000) return null;
  return Math.round(ms);
}

export function formatInterval(ms: number): string {
  if (ms % 60_000 === 0) return `${ms / 60_000}m`;
  if (ms % 1000 === 0) return `${ms / 1000}s`;
  return `${ms}ms`;
}

/** Timestamps and durations. Never coloured, never abbreviated past legibility. */

/** `14:02:31`, in the viewer's zone. The list and the meta line's spelling. */
export function clock(ms: number | null | undefined): string {
  if (ms === null || ms === undefined) return '';
  const d = new Date(ms);
  const p = (n: number) => String(n).padStart(2, '0');
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

/** `14:02:31` plus the date when it is not today — a console outlives a day. */
export function stamp(ms: number | null | undefined): string {
  if (ms === null || ms === undefined) return '';
  const d = new Date(ms);
  const today = new Date();
  const sameDay =
    d.getFullYear() === today.getFullYear() &&
    d.getMonth() === today.getMonth() &&
    d.getDate() === today.getDate();
  if (sameDay) return clock(ms);
  const p = (n: number) => String(n).padStart(2, '0');
  return `${p(d.getMonth() + 1)}-${p(d.getDate())} ${clock(ms)}`;
}

/** `120ms`, `1.4s`, `4m 12s`, `1h 03m`. */
export function duration(ms: number): string {
  if (!Number.isFinite(ms) || ms < 0) return '';
  const s = ms / 1000;
  if (s < 1) return `${Math.round(ms)}ms`;
  if (s < 60) return `${s.toFixed(1)}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m ${String(Math.round(s % 60)).padStart(2, '0')}s`;
  return `${Math.floor(s / 3600)}h ${String(Math.floor((s % 3600) / 60)).padStart(2, '0')}m`;
}

/** The ruler's tick labels: offsets from the span's start. */
export function tickLabel(ms: number): string {
  if (ms === 0) return '0s';
  return duration(ms);
}

/** Decode a promise's `param`/`value` for the inspector. */
export function decodeBody(data: string | undefined | null): string {
  if (!data) return '';
  let text = data;
  try {
    // The SDKs base64 the payload; anything else is shown as it arrived.
    const bytes = Uint8Array.from(atob(data), (c) => c.charCodeAt(0));
    text = new TextDecoder().decode(bytes);
  } catch {
    /* not base64 — fall through with the raw string */
  }
  try {
    return JSON.stringify(JSON.parse(text), null, 2);
  } catch {
    return text;
  }
}

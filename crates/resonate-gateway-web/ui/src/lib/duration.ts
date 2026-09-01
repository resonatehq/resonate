/**
 * `1h`, `30s`, `1h30m`, `100ms` — the spelling the CLI takes.
 *
 * A deliberate mirror of `parse_duration` in `src/cli.rs`, down to which units
 * exist and which inputs are refused: an operator who knows what
 * `resonate invoke -t 90s` means should not have to learn a second dialect to
 * type the same thing here.
 *
 * Returns the duration in milliseconds, or a message saying what is wrong with
 * it — the same shape of message the CLI prints.
 */
export function parseDuration(input: string): { ms: number } | { error: string } {
  const s = input.trim();
  if (s === '') return { error: `Invalid duration '${input}': expected a number` };

  const UNITS: Record<string, number> = {
    ms: 1,
    s: 1_000,
    m: 60_000,
    h: 3_600_000,
    d: 86_400_000
  };

  let total = 0;
  let i = 0;
  while (i < s.length) {
    const numStart = i;
    while (i < s.length && s[i] >= '0' && s[i] <= '9') i++;
    if (i === numStart) return { error: `Invalid duration '${input}': expected a number` };
    const n = Number(s.slice(numStart, i));
    if (!Number.isSafeInteger(n)) return { error: `Invalid number in duration '${input}'` };

    const unitStart = i;
    while (i < s.length && /[a-zA-Z]/.test(s[i])) i++;
    const unit = s.slice(unitStart, i);
    if (unit === '') return { error: `Missing unit in duration '${input}'` };
    const scale = UNITS[unit];
    if (scale === undefined) return { error: `Unknown unit '${unit}' in duration '${input}'` };

    total += n * scale;
  }

  if (total === 0 && s !== '0') return { error: `Invalid duration '${input}'` };
  return { ms: total };
}

/** UTF-8 safe base64 — `btoa` alone throws on anything outside Latin-1. */
export function base64(text: string): string {
  const bytes = new TextEncoder().encode(text);
  let binary = '';
  for (const b of bytes) binary += String.fromCharCode(b);
  return btoa(binary);
}

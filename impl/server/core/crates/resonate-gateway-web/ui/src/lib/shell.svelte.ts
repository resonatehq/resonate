/**
 * What the shell shows and what the pages put there.
 *
 * The header is one bar across every screen — title, search, Refresh — so its
 * contents belong to the shell, not to whichever page happens to be mounted.
 * A page sets the title, says whether search applies, and hands over what
 * Refresh should do.
 */

export const shell = $state({
  /** The `<h1>`. Always Inter, even when it is an id — a heading is a heading. */
  title: 'Durable Executions',
  /** The 12.5px subline under it. Empty on every screen the design specifies. */
  sub: '',
  /** The search box's text. Matches id, function and tag, on what is loaded. */
  query: '',
  /** Whether the search box is shown at all. */
  searchable: true,
  /** What the Refresh button does on this screen, when it does anything. */
  refresh: null as null | (() => void),
  /**
   * The screen's one primary action, when it has one.
   *
   * Only the executions list does: Invoke. It sits in the header because that
   * is where the shell's controls are, and because starting an execution is not
   * about any row in the table below it.
   */
  action: null as null | { label: string; run: () => void },
  /** A request is in flight. */
  busy: false
});

export function setPage(opts: {
  title: string;
  sub?: string;
  searchable?: boolean;
  refresh?: () => void;
  action?: { label: string; run: () => void };
}) {
  shell.title = opts.title;
  shell.sub = opts.sub ?? '';
  shell.searchable = opts.searchable ?? true;
  shell.action = opts.action ?? null;
  // No refresh, no button: a control that does nothing is worse than no
  // control. Settings is the screen that has nothing to re-read.
  shell.refresh = opts.refresh ?? null;
}

/** Does this row match what is typed in the search box? */
export function matches(query: string, ...fields: (string | null | undefined)[]): boolean {
  const q = query.trim().toLowerCase();
  if (!q) return true;
  return fields.some((f) => f && f.toLowerCase().includes(q));
}

/**
 * Put the reader back at the top of the content region.
 *
 * Paging keeps the scroll position otherwise, which lands you halfway down a
 * page you have not read — and on a shorter page, at its end.
 */
export function scrollToTop() {
  document.getElementById('console-content')?.scrollTo({ top: 0 });
}

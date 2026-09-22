<script lang="ts">
  import { base } from '$app/paths';
  import { untrack } from 'svelte';
  import { goto } from '$app/navigation';
  import { searchExecutions, RpcError } from '$lib/api';
  import { settings } from '$lib/settings.svelte';
  import { shell, setPage, matches, scrollToTop } from '$lib/shell.svelte';
  import { stamp } from '$lib/format';
  import { STATE_FILTERS, type ExecutionItem, type PromiseState } from '$lib/types';
  import Status from '$lib/components/Status.svelte';
  import ErrorPanel from '$lib/components/ErrorPanel.svelte';
  import StaleNotice from '$lib/components/StaleNotice.svelte';
  import Empty from '$lib/components/Empty.svelte';
  import Loading from '$lib/components/Loading.svelte';
  import InvokeDialog from '$lib/components/InvokeDialog.svelte';

  /**
   * Find the execution you care about.
   *
   * Three filters go to the server, where they are exact over every page:
   * status, function, and the window. Search is client-side over what is
   * loaded, which is what the design asks for — it matches id, function and
   * tag, and it does not pretend to have seen page two.
   */

  const WINDOWS = [
    { label: 'Last 1h', ms: 3_600_000 },
    { label: 'Last 24h', ms: 86_400_000 },
    { label: 'Last 3d', ms: 259_200_000 },
    { label: 'Last 7d', ms: 604_800_000 }
  ];

  let status = $state<PromiseState | 'all'>('all');
  let func = $state('all');
  let windowLabel = $state('Last 24h');

  let items = $state<ExecutionItem[]>([]);
  let total = $state<number | null>(null);
  /** Cursors for the pages already walked, so Previous is a step back. */
  let cursors = $state<(string | undefined)[]>([undefined]);
  let pageIndex = $state(0);
  let nextCursor = $state<string | undefined>(undefined);

  let loading = $state(true);
  let error = $state<{ message: string; code?: string } | null>(null);
  /** The last answer that landed; what "stale" is measured against. */
  let loadedAt = $state(0);
  let staleError = $state<string | null>(null);

  let inflight: AbortController | null = null;

  const windowMs = $derived(WINDOWS.find((w) => w.label === windowLabel)?.ms ?? 86_400_000);

  /**
   * The function dropdown offers what is on screen.
   *
   * A distinct-values request would be a fourth `ui.*` read for a filter over
   * a page of fifty; the union of what has loaded is the honest offer, and
   * picking one sends the filter to the server where it is exact.
   */
  const funcs = $derived(
    Array.from(new Set(items.map((i) => i.func).filter((f): f is string => !!f))).sort()
  );

  const visible = $derived(
    items.filter((i) =>
      matches(shell.query, i.id, i.func, ...Object.entries(i.tags).map(([k, v]) => `${k}:${v}`))
    )
  );

  async function load(cursor?: string, opts: { silent?: boolean } = {}) {
    inflight?.abort();
    const ctl = new AbortController();
    inflight = ctl;
    if (!opts.silent) loading = items.length === 0;
    shell.busy = true;
    try {
      const page = await searchExecutions(
        {
          state: status === 'all' ? undefined : [status],
          func: func === 'all' ? undefined : func,
          createdFrom: Date.now() - windowMs,
          sort: 'createdAt:desc',
          limit: 50,
          cursor,
          countTotal: true
        },
        ctl.signal
      );
      items = page.items;
      total = page.total ?? null;
      nextCursor = page.cursor;
      loadedAt = Date.now();
      error = null;
      staleError = null;
    } catch (e) {
      if (ctl.signal.aborted) return;
      const err = e as RpcError;
      // A cursor minted under another sort, or one the server no longer
      // honours: page one is always valid, so go back to it rather than
      // showing the operator a dead end.
      if (err instanceof RpcError && err.isCursorMismatch && pageIndex > 0) {
        cursors = [undefined];
        pageIndex = 0;
        void load(undefined);
        return;
      }
      if (items.length > 0) staleError = err.message;
      else error = { message: err.message, code: err instanceof RpcError ? err.code : undefined };
    } finally {
      if (inflight === ctl) {
        inflight = null;
        shell.busy = false;
        loading = false;
      }
    }
  }

  /** Filters reset pagination: a page two of a different question is nonsense. */
  function refilter() {
    cursors = [undefined];
    pageIndex = 0;
    void load(undefined);
  }

  function next() {
    if (!nextCursor) return;
    cursors = [...cursors.slice(0, pageIndex + 1), nextCursor];
    pageIndex += 1;
    scrollToTop();
    void load(nextCursor);
  }

  function previous() {
    if (pageIndex === 0) return;
    pageIndex -= 1;
    scrollToTop();
    void load(cursors[pageIndex]);
  }

  let invoking = $state(false);

  setPage({
    title: 'Durable Executions',
    refresh: () => load(cursors[pageIndex]),
    action: { label: 'Invoke', run: () => (invoking = true) }
  });

  $effect(() => {
    // Once, on mount. `load` reads state on its way in, so calling it tracked
    // would make this effect depend on what it writes and re-run forever;
    // every later load is driven by a filter, the pager, or the poll below.
    untrack(() => void load(undefined));
    return () => inflight?.abort();
  });

  // Page one only: an operator reading page three is not interrupted, and a
  // silent poll holds the last good answer rather than blanking the screen.
  $effect(() => {
    const ms = settings.pollMs;
    if (pageIndex !== 0) return;
    const t = setInterval(() => load(undefined, { silent: true }), ms);
    return () => clearInterval(t);
  });

  function settledCell(row: ExecutionItem): string {
    if (row.state === 'pending' || row.state === 'rejected_timedout') return '';
    return row.settledAt ? stamp(row.settledAt) : '';
  }

  const countNote = $derived.by(() => {
    if (total === null) return `${visible.length} execution${visible.length === 1 ? '' : 's'}`;
    if (shell.query.trim()) return `${visible.length} of ${total} shown`;
    if (total > items.length) return `${items.length} of ${total} executions`;
    return `${total} execution${total === 1 ? '' : 's'}`;
  });
</script>

{#if invoking}
  <InvokeDialog
    onclose={() => (invoking = false)}
    oncreated={(id) => {
      invoking = false;
      goto(`${base}/executions/${encodeURIComponent(id)}`);
    }}
  />
{/if}

<section>
  <div class="filters wrap-narrow">
    <label class="select">
      <span>Status</span>
      <select
        bind:value={status}
        onchange={refilter}
        aria-label="Filter by status"
      >
        {#each STATE_FILTERS as f (f.value)}
          <option value={f.value}>{f.label}</option>
        {/each}
      </select>
    </label>

    <label class="select">
      <span>Function</span>
      <select bind:value={func} onchange={refilter} aria-label="Filter by function">
        <option value="all">All</option>
        {#each funcs as f (f)}
          <option value={f}>{f}</option>
        {/each}
      </select>
    </label>

    <label class="select">
      <span>Window</span>
      <select bind:value={windowLabel} onchange={refilter} aria-label="Filter by time window">
        {#each WINDOWS as w (w.label)}
          <option value={w.label}>{w.label}</option>
        {/each}
      </select>
    </label>

    <div class="count">{countNote}</div>
  </div>

  {#if staleError}
    <StaleNotice since={loadedAt} message={staleError} />
  {/if}

  {#if error}
    <div class="pad"><ErrorPanel message={error.message} code={error.code} onretry={() => load(cursors[pageIndex])} /></div>
  {:else if loading}
    <Loading what="Loading executions…" />
  {:else if items.length === 0}
    <Empty
      title="No executions in this window"
      line="Nothing has been started here in the last {windowLabel.replace('Last ', '')}. Widen the window, or clear the status filter."
    />
  {:else if visible.length === 0}
    <Empty
      title="Nothing matches “{shell.query}”"
      line="The search runs over the {items.length} execution{items.length === 1 ? '' : 's'} on this page. Clear it, or page further back."
    />
  {:else}
    <div class="scroll">
      <div class="head">
        <div>Status</div>
        <div>Execution id</div>
        <div>Function</div>
        <div>Created at</div>
        <div>Settled at</div>
        <div>Timeout at</div>
      </div>
      {#each visible as row (row.id)}
        <div
          class="row"
          role="button"
          tabindex="0"
          onclick={() => goto(`${base}/executions/${encodeURIComponent(row.id)}`)}
          onkeydown={(e) => {
            if (e.key === 'Enter' || e.key === ' ') {
              e.preventDefault();
              void goto(`${base}/executions/${encodeURIComponent(row.id)}`);
            }
          }}
        >
          <Status state={row.state} />
          <div class="id mono">{row.id}</div>
          <div class="func">{row.func ?? ''}</div>
          <div class="ts">{stamp(row.createdAt)}</div>
          <!-- Empty, not a dash, when there is nothing to say. A timed-out
               promise settles *at* its deadline, so printing it here would put
               the same timestamp in two adjacent columns. -->
          <div class="ts">{settledCell(row)}</div>
          <div class="ts">{stamp(row.timeoutAt)}</div>
        </div>
      {/each}
    </div>

    {#if pageIndex > 0 || nextCursor}
      <div class="pager wrap-narrow">
        <button class="btn" onclick={previous} disabled={pageIndex === 0}>← Newer</button>
        <span class="page">Page {pageIndex + 1}</span>
        <button class="btn" onclick={next} disabled={!nextCursor}>Older →</button>
      </div>
    {/if}
  {/if}
</section>

<style>
  section {
    padding: 0 0 40px;
  }

  .filters {
    display: flex;
    align-items: center;
    gap: 9px;
    padding: 14px 26px;
    border-bottom: 1px solid var(--line);
    flex-wrap: wrap;
  }
  .select {
    display: flex;
    align-items: center;
    border: 1px solid var(--line);
    border-radius: 6px;
    background: var(--panel);
    height: 32px;
    overflow: hidden;
  }
  .select span {
    font-size: 12.5px;
    color: var(--dim);
    padding: 0 9px;
    border-right: 1px solid var(--line);
    line-height: 30px;
    background: var(--elev);
  }
  .select select {
    border: 0;
    background: transparent;
    color: var(--text);
    font-size: 12.5px;
    padding: 0 8px;
    height: 30px;
    cursor: pointer;
  }
  .count {
    font-size: 13px;
    color: var(--dim);
    padding-left: 5px;
  }

  .scroll {
    overflow-x: auto;
  }
  .head,
  .row {
    display: grid;
    grid-template-columns: 118px minmax(200px, 1fr) minmax(200px, 1fr) 104px 104px 104px;
    gap: 18px;
    min-width: 920px;
    padding: 0 26px;
  }
  .head {
    padding: 11px 26px;
    border-bottom: 1px solid var(--line);
    font-size: 11.5px;
    font-weight: 600;
    color: var(--dim);
  }
  .row {
    height: 44px;
    align-items: center;
    border-bottom: 1px solid var(--line);
    cursor: pointer;
  }
  .row:hover,
  .row:focus-visible {
    background: var(--elev);
    outline: none;
  }
  .id {
    font-size: 12.5px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .func {
    font-size: 13px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .pad {
    padding: 24px 26px;
  }
  .pager {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 16px 26px;
  }
  .page {
    font-size: 12.5px;
    color: var(--faint);
  }
</style>

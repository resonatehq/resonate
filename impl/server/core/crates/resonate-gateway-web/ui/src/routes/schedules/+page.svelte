<script lang="ts">
  import { untrack } from 'svelte';
  import { searchSchedules, RpcError } from '$lib/api';
  import { settings } from '$lib/settings.svelte';
  import { shell, setPage, matches, scrollToTop } from '$lib/shell.svelte';
  import { stamp } from '$lib/format';
  import type { ScheduleItem } from '$lib/types';
  import ErrorPanel from '$lib/components/ErrorPanel.svelte';
  import StaleNotice from '$lib/components/StaleNotice.svelte';
  import Empty from '$lib/components/Empty.svelte';
  import Loading from '$lib/components/Loading.svelte';

  /**
   * Schedules: a table and nothing else.
   *
   * Every timestamp is muted — next run is not accent-coloured, and there is no
   * "last result" column, because a schedule's last firing is an execution and
   * belongs on the executions screen.
   */

  let items = $state<ScheduleItem[]>([]);
  let total = $state<number | null>(null);
  let cursors = $state<(string | undefined)[]>([undefined]);
  let pageIndex = $state(0);
  let nextCursor = $state<string | undefined>(undefined);

  let loading = $state(true);
  let error = $state<{ message: string; code?: string } | null>(null);
  let loadedAt = $state(0);
  let staleError = $state<string | null>(null);
  let inflight: AbortController | null = null;

  const visible = $derived(
    items.filter((s) =>
      matches(shell.query, s.id, s.cron, s.promiseId, ...Object.entries(s.tags).map(([k, v]) => `${k}:${v}`))
    )
  );

  async function load(cursor?: string, opts: { silent?: boolean } = {}) {
    inflight?.abort();
    const ctl = new AbortController();
    inflight = ctl;
    if (!opts.silent) loading = items.length === 0;
    shell.busy = true;
    try {
      const page = await searchSchedules(
        { sort: 'nextRunAt:asc', limit: 50, cursor, countTotal: true },
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

  setPage({ title: 'Schedules', refresh: () => load(cursors[pageIndex]) });

  $effect(() => {
    // Once, on mount — see the executions list for why this is untracked.
    untrack(() => void load(undefined));
    return () => inflight?.abort();
  });

  $effect(() => {
    const ms = settings.pollMs;
    if (pageIndex !== 0) return;
    const t = setInterval(() => load(undefined, { silent: true }), ms);
    return () => clearInterval(t);
  });
</script>

<section>
  {#if staleError}
    <div class="stale"><StaleNotice since={loadedAt} message={staleError} /></div>
  {/if}

  {#if error}
    <ErrorPanel message={error.message} code={error.code} onretry={() => load(cursors[pageIndex])} />
  {:else if loading}
    <Loading what="Loading schedules…" />
  {:else if items.length === 0}
    <Empty
      title="No schedules"
      line="Nothing recurring is registered on this server. A schedule is created with schedule.create and fires a promise on its cron."
    />
  {:else if visible.length === 0}
    <Empty
      title="Nothing matches “{shell.query}”"
      line="The search runs over the {items.length} schedule{items.length === 1 ? '' : 's'} on this page."
    />
  {:else}
    <div class="card table">
      <div class="head">
        <div>Schedule</div>
        <div>Cron</div>
        <div>Next run</div>
        <div>Last run</div>
        <div>Promise id</div>
      </div>
      {#each visible as s (s.id)}
        <div class="row">
          <div class="name">{s.id}</div>
          <div class="cron">{s.cron}</div>
          <div class="when">{stamp(s.nextRunAt)}</div>
          <div class="when">{s.lastRunAt ? stamp(s.lastRunAt) : ''}</div>
          <div class="pid">{s.promiseId}</div>
        </div>
      {/each}
    </div>

    {#if pageIndex > 0 || nextCursor}
      <div class="pager">
        <button class="btn" onclick={previous} disabled={pageIndex === 0}>← Previous</button>
        <span class="page">
          Page {pageIndex + 1}{total !== null ? ` · ${total} schedule${total === 1 ? '' : 's'}` : ''}
        </span>
        <button class="btn" onclick={next} disabled={!nextCursor}>Next →</button>
      </div>
    {/if}
  {/if}
</section>

<style>
  section {
    padding: 24px 26px 40px;
  }
  .stale {
    margin: -24px -26px 24px;
  }
  .table {
    overflow-x: auto;
  }
  .head,
  .row {
    display: grid;
    grid-template-columns: minmax(180px, 1.4fr) 150px 110px 110px minmax(200px, 1.4fr);
    gap: 14px;
    min-width: 900px;
    padding: 11px 18px;
    border-bottom: 1px solid var(--line);
    align-items: center;
  }
  .row:last-child {
    border-bottom: 0;
  }
  .head {
    padding: 10px 18px;
    font-family: var(--mono);
    font-size: 10.5px;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    color: var(--faint);
  }
  .row:hover {
    background: var(--elev);
  }
  .name {
    font-size: 13.5px;
    font-weight: 500;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .cron {
    font-family: var(--mono);
    font-size: 12.5px;
    color: var(--dim);
  }
  /* Timestamps are never coloured, here or anywhere. */
  .when {
    font-size: 12.5px;
    color: var(--dim);
    white-space: nowrap;
  }
  .pid {
    font-family: var(--mono);
    font-size: 11.5px;
    color: var(--faint);
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .pager {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 16px 0 0;
  }
  .page {
    font-size: 12.5px;
    color: var(--faint);
  }

  @media (max-width: 860px) {
    section {
      padding: 16px 14px 40px;
    }
    .stale {
      margin: -16px -14px 16px;
    }
  }
</style>

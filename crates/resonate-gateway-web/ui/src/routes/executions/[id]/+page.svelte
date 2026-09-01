<script lang="ts">
  import { base } from '$app/paths';
  import { page } from '$app/state';
import { untrack } from 'svelte';
  import { goto, replaceState } from '$app/navigation';
  import { getExecution, cancelExecution, RpcError } from '$lib/api';
  import { settings } from '$lib/settings.svelte';
  import { shell, setPage } from '$lib/shell.svelte';
  import { clock, stamp, duration, decodeBody, tickLabel } from '$lib/format';
  import { rows as buildRows, span as buildSpan, bar, shortName } from '$lib/tree';
  import { STATE_LABEL, type ExecutionView, type ExecutionNode } from '$lib/types';
  import Status from '$lib/components/Status.svelte';
  import Dot from '$lib/components/Dot.svelte';
  import ErrorPanel from '$lib/components/ErrorPanel.svelte';
  import StaleNotice from '$lib/components/StaleNotice.svelte';
  import Loading from '$lib/components/Loading.svelte';

  /**
   * Understand what an execution did and where it is stuck.
   *
   * One request builds the whole screen — the tree, the tasks and the root —
   * so what is on screen is one consistent read rather than a fan-out that can
   * disagree with itself halfway down.
   */

  const id = $derived(decodeURIComponent(page.params.id ?? ''));

  /**
   * The selected step.
   *
   * Local, with the URL kept in step beside it rather than driving it: `?step=`
   * exists so a selection can be sent to someone, and reading it back on every
   * render would tie what is on screen to the router's idea of the query
   * string. Arriving with one selects it; clicking updates it.
   */
  let selectedId = $state<string | null>(null);

  let view = $state<ExecutionView | null>(null);
  let loading = $state(true);
  let error = $state<{ message: string; code?: string } | null>(null);
  let loadedAt = $state(0);
  let staleError = $state<string | null>(null);
  let cancelling = $state(false);
  let cancelError = $state<string | null>(null);

  /** The right edge of a running bar is *now*, so it has to keep moving. */
  let now = $state(Date.now());

  let inflight: AbortController | null = null;

  async function load(opts: { silent?: boolean } = {}) {
    if (!id) return;
    inflight?.abort();
    const ctl = new AbortController();
    inflight = ctl;
    if (!opts.silent) loading = view === null;
    shell.busy = true;
    try {
      const next = await getExecution(id, ctl.signal);
      view = next;
      now = Date.now();
      loadedAt = now;
      error = null;
      staleError = null;
    } catch (e) {
      if (ctl.signal.aborted) return;
      const err = e as RpcError;
      if (view) staleError = err.message;
      else error = { message: err.message, code: err instanceof RpcError ? err.code : undefined };
    } finally {
      if (inflight === ctl) {
        inflight = null;
        shell.busy = false;
        loading = false;
      }
    }
  }

  $effect(() => {
    // The id in the URL is the *only* thing that should restart this. `load`
    // reads state on its way in, and a bare call here would make the effect
    // depend on what it writes — one load, one re-run, forever.
    void id;
    untrack(() => {
      view = null;
      error = null;
      // A step named in the URL is the selection to open with; arriving at a
      // different execution starts with nothing selected, as the design asks.
      selectedId = page.url.searchParams.get('step');
      void load();
    });
    return () => inflight?.abort();
  });

  $effect(() => {
    const title = id;
    untrack(() => setPage({ title, searchable: false, refresh: () => load() }));
  });

  $effect(() => {
    const ms = settings.pollMs;
    const t = setInterval(() => load({ silent: true }), ms);
    return () => clearInterval(t);
  });

  // A running bar's right edge is the clock, not the last response.
  $effect(() => {
    if (!view || view.root.settledAt) return;
    const t = setInterval(() => (now = Date.now()), 1000);
    return () => clearInterval(t);
  });

  const rows = $derived(view ? buildRows(view) : []);
  const span = $derived(view ? buildSpan(view, now) : null);
  const selected = $derived<ExecutionNode | null>(
    view && selectedId ? (view.nodes.find((n) => n.id === selectedId) ?? null) : null
  );

  function select(nodeId: string) {
    selectedId = nodeId;
    syncUrl(nodeId);
  }

  function deselect() {
    selectedId = null;
    syncUrl(null);
  }

  /** Keep `?step=` in the address bar, without a navigation. */
  function syncUrl(step: string | null) {
    const url = new URL(page.url);
    if (step) url.searchParams.set('step', step);
    else url.searchParams.delete('step');
    replaceState(url, page.state);
  }

  async function cancel() {
    if (!view) return;
    if (!confirm(`Cancel ${view.root.id}? This settles the execution as canceled.`)) return;
    cancelling = true;
    cancelError = null;
    try {
      await cancelExecution(view.root.id);
      await load({ silent: true });
    } catch (e) {
      cancelError = (e as Error).message;
    } finally {
      cancelling = false;
    }
  }

  const meta = $derived.by(() => {
    if (!view) return '';
    const r = view.root;
    const created = `created ${stamp(r.createdAt)}`;
    if (r.settledAt) return `${created} · settled ${stamp(r.settledAt)}`;
    return `${created} · times out ${stamp(r.timeoutAt)}`;
  });
</script>

<section>
  <button class="back" onclick={() => goto(`${base}/executions`)}>← All executions</button>

  {#if error}
    <ErrorPanel
      title={error.code === 'not_found' ? 'No such execution' : 'Could not load this execution'}
      message={error.code === 'not_found'
        ? `Nothing here is called ${id}. It may have been created under a different id, or removed.`
        : error.message}
      code={error.code}
      onretry={() => load()}
    />
  {:else if loading || !view || !span}
    <Loading what="Loading execution…" />
  {:else}
    <div class="headline">
      <h2>{view.nodes.find((n) => n.id === view.root.id)?.func ?? view.root.id}</h2>
      <Status state={view.root.state} />
      <div class="grow"></div>
      <button
        class="btn-danger"
        onclick={cancel}
        disabled={cancelling || view.root.state !== 'pending'}
        title={view.root.state !== 'pending' ? 'Already settled' : 'Cancel this execution'}
      >
        {cancelling ? 'Cancelling…' : 'Cancel execution'}
      </button>
    </div>

    <div class="meta">{meta}</div>

    {#if cancelError}
      <div class="cancel-error"><ErrorPanel title="Cancel failed" message={cancelError} /></div>
    {/if}
    {#if staleError}
      <div class="stale-wrap"><StaleNotice since={loadedAt} message={staleError} /></div>
    {/if}
    {#if view.truncated}
      <div class="truncated">
        This tree is larger than the console asked for; the rows below are the first
        {view.nodes.length} by creation time.
      </div>
    {/if}

    <div class="cols">
      <div class="tree card">
        <div class="rows-scroll">
          <div class="ruler">
            <div class="namecol"></div>
            <div class="track">
              {#each span.ticks as t (t)}<span>{tickLabel(t)}</span>{/each}
            </div>
          </div>

          {#each rows as row (row.key)}
            {#if row.kind === 'task'}
              <!-- A task header owns every row beneath it until the tree returns
                   to its own depth. Only the word: no id (it shares its
                   promise's), no state, no pid. The boundary is the content. -->
              <div class="taskrow">
                <div class="namecol" style="padding-left:{row.indent}px">
                  <span class="taskword">task</span>
                </div>
                <div class="track"></div>
                <div class="dur"></div>
              </div>
            {:else}
              {@const b = bar(row.node, span, now)}
              {@const unsettled = row.node.state === 'pending'}
              {@const rejected = row.node.state === 'rejected'}
              <div
                class="steprow"
                class:on={selectedId === row.node.id}
                role="button"
                tabindex="0"
                onclick={() => select(row.node.id)}
                onkeydown={(e) => {
                  if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    select(row.node.id);
                  }
                }}
              >
                <div class="namecol" style="padding-left:{row.indent}px">
                  <Dot state={row.node.state} />
                  <span class="name">{row.node.func ?? shortName(row.node, view.root.id)}</span>
                </div>
                <div class="track bars">
                  <div class="axis"></div>
                  <div
                    class="bar"
                    class:hollow={unsettled}
                    class:err={rejected}
                    style="left:{b.left}%;width:{b.width}%"
                  ></div>
                </div>
                <div class="dur">
                  {duration((row.node.settledAt ?? now) - row.node.createdAt)}
                </div>
              </div>
            {/if}
          {/each}
        </div>
      </div>

      {#if selected}
        <aside class="inspector card">
          <div class="ins-head">
            <div class="ins-title">
              <Dot state={selected.state} />
              <div class="ins-name">{selected.func ?? shortName(selected, view.root.id)}</div>
              <div class="grow"></div>
              <span class="ins-state" class:err={selected.state === 'rejected'}>
                {STATE_LABEL[selected.state]}
              </span>
              <button class="close" onclick={deselect} aria-label="Close inspector">✕</button>
            </div>
            <div class="ins-id">{selected.id}</div>
          </div>

          <div class="ins-body">
            <div>
              <div class="label">Param</div>
              <pre>{decodeBody(selected.param.data) || '—'}</pre>
            </div>

            {#if selected.value.data}
              <div>
                <div class="label">
                  {selected.state === 'rejected' ? 'Value · error' : 'Value'}
                </div>
                <pre class:err={selected.state === 'rejected'}>{decodeBody(selected.value.data)}</pre>
              </div>
            {/if}

            <div class="stamps">
              <div class="stamp">
                <div class="k">Created at</div>
                <div class="v">{clock(selected.createdAt)}</div>
              </div>
              <div class="stamp">
                <div class="k">Settled at</div>
                <div class="v">{selected.settledAt ? clock(selected.settledAt) : ''}</div>
              </div>
              <div class="stamp">
                <div class="k">Timeout at</div>
                <div class="v">{clock(selected.timeoutAt)}</div>
              </div>
            </div>
          </div>
        </aside>
      {/if}
    </div>
  {/if}
</section>


<style>
  section {
    padding: 20px 26px 40px;
  }
  .back {
    border: 0;
    background: transparent;
    color: var(--faint);
    font-size: 12.5px;
    cursor: pointer;
    padding: 0 0 12px;
  }
  .back:hover {
    color: var(--text);
  }

  .headline {
    display: flex;
    align-items: center;
    gap: 12px;
    flex-wrap: wrap;
    margin-bottom: 6px;
  }
  h2 {
    margin: 0;
    font-size: 18px;
    font-weight: 600;
    letter-spacing: -0.015em;
    line-height: 1.2;
  }
  .grow {
    flex: 1;
  }
  .meta {
    font-size: 12.5px;
    color: var(--faint);
    margin-bottom: 18px;
  }
  .cancel-error,
  .stale-wrap {
    margin-bottom: 18px;
  }
  .truncated {
    font-size: 12.5px;
    color: var(--warn-fg);
    background: var(--warn-bg);
    border-radius: 6px;
    padding: 9px 12px;
    margin-bottom: 18px;
    max-width: 60ch;
  }

  .cols {
    display: flex;
    gap: 18px;
    align-items: flex-start;
    flex-wrap: wrap;
  }

  .tree {
    flex: 1;
    min-width: 480px;
    overflow: hidden;
  }
  .rows-scroll {
    overflow-x: auto;
  }

  .ruler {
    display: flex;
    padding: 8px 18px 4px;
    border-bottom: 1px solid var(--line);
    min-width: 1310px;
  }
  .ruler .namecol {
    background: var(--panel);
    z-index: 2;
  }
  .track {
    width: 900px;
    flex: none;
    display: flex;
    justify-content: space-between;
    font-family: var(--mono);
    font-size: 10px;
    color: var(--faint);
  }

  .namecol {
    width: 300px;
    flex: none;
    display: flex;
    align-items: center;
    gap: 9px;
    min-width: 0;
    position: sticky;
    left: 0;
    z-index: 1;
    background: var(--panel);
  }

  .taskrow {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 0 18px;
    height: 28px;
    background: var(--elev);
    min-width: 1310px;
  }
  .taskrow .namecol {
    background: var(--elev);
    gap: 8px;
  }
  .taskword {
    font-family: var(--mono);
    font-size: 10.5px;
    letter-spacing: 0.04em;
    color: var(--faint);
    white-space: nowrap;
  }
  .steprow {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 0 18px;
    height: var(--row);
    cursor: pointer;
    border-bottom: 1px solid var(--line);
    min-width: 1310px;
    background: transparent;
  }
  .steprow:hover,
  .steprow:focus-visible {
    background: var(--elev);
    outline: none;
  }
  .steprow:hover .namecol,
  .steprow:focus-visible .namecol {
    background: var(--elev);
  }
  .steprow.on {
    background: var(--elev);
    box-shadow: inset 2px 0 0 var(--dim);
  }
  .steprow.on .namecol {
    background: var(--elev);
  }
  .name {
    font-size: 13px;
    font-weight: 500;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .bars {
    position: relative;
    height: 16px;
    display: block;
  }
  .axis {
    position: absolute;
    inset: 7px 0 auto;
    height: 1px;
    background: var(--line);
  }
  .bar {
    position: absolute;
    top: 4px;
    height: 8px;
    border-radius: 3px;
    box-sizing: border-box;
    background: var(--text);
  }
  .bar.err {
    background: var(--err);
  }
  .bar.hollow {
    background: transparent;
    border: 1px solid var(--dim);
  }
  .bar.hollow.err {
    border-color: var(--err);
  }

  .dur {
    width: 74px;
    flex: none;
    text-align: right;
    font-family: var(--mono);
    font-size: 11.5px;
    color: var(--dim);
  }

  /* --- inspector --------------------------------------------------------- */

  .inspector {
    width: 394px;
    flex: none;
    overflow: hidden;
  }
  .ins-head {
    padding: 16px 18px;
    border-bottom: 1px solid var(--line);
  }
  .ins-title {
    display: flex;
    align-items: center;
    gap: 10px;
  }
  .ins-name {
    font-size: 15px;
    font-weight: 600;
    letter-spacing: -0.01em;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .ins-state {
    font-size: 12.5px;
    color: var(--dim);
    white-space: nowrap;
  }
  .ins-state.err {
    color: var(--err-fg);
  }
  .close {
    border: 0;
    background: transparent;
    color: var(--faint);
    font-size: 15px;
    line-height: 1;
    cursor: pointer;
    padding: 2px 0 2px 4px;
  }
  .close:hover {
    color: var(--text);
  }
  .ins-id {
    font-family: var(--mono);
    font-size: 11.5px;
    color: var(--faint);
    margin-top: 8px;
    word-break: break-all;
  }

  .ins-body {
    padding: 16px 18px 18px;
    display: flex;
    flex-direction: column;
    gap: 14px;
  }
  .label {
    font-size: 11.5px;
    font-weight: 600;
    color: var(--dim);
    margin-bottom: 7px;
  }
  pre {
    font-family: var(--mono);
    font-size: 11.5px;
    line-height: 1.6;
    color: var(--dim);
    background: var(--bg);
    border: 1px solid var(--line);
    border-radius: 6px;
    padding: 11px 13px;
    white-space: pre-wrap;
    word-break: break-word;
    max-height: 320px;
    overflow: auto;
  }
  pre.err {
    color: var(--err-fg);
    border-color: var(--err-bg);
  }

  .stamps {
    display: flex;
    flex-direction: column;
    gap: 10px;
    padding-top: 14px;
    border-top: 1px solid var(--line);
  }
  .stamp {
    display: flex;
    justify-content: space-between;
    gap: 14px;
    align-items: baseline;
  }
  .stamp .k {
    font-size: 12.5px;
    color: var(--faint);
  }
  .stamp .v {
    font-size: 12.5px;
    font-family: var(--mono);
    text-align: right;
    color: var(--dim);
  }

  @media (max-width: 860px) {
    section {
      padding: 16px 14px 40px;
    }
    .tree {
      min-width: 0;
      width: 100%;
    }
    .inspector {
      width: 100%;
    }
  }
</style>

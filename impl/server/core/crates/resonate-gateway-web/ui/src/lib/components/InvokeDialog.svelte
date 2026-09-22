<script lang="ts">
  import { getPromise, invoke, RpcError, type InvokeRequest } from '$lib/api';
  import { parseDuration } from '$lib/duration';

  /**
   * Start a durable execution.
   *
   * The same act as `resonate invoke`, and deliberately the same fields in the
   * same order, so an operator who knows the command knows this form. What it
   * sends is an ordinary `promise.create` — there is no console-only request
   * behind this button.
   *
   * Everything is validated here before anything is sent, because a promise is
   * durable: a typo in the timeout is a row that sits there until its deadline.
   */

  let { onclose, oncreated }: { onclose: () => void; oncreated: (id: string) => void } = $props();

  let id = $state('');
  let func = $state('');
  let args = $state('[]');
  let target = $state('poll://any@default');
  let timeout = $state('1h');
  let version = $state('1');
  let delay = $state('');

  let busy = $state(false);
  let error = $state<string | null>(null);
  /** Set when the id is taken: the offer to open what is already there. */
  let clash = $state<string | null>(null);

  let firstField: HTMLInputElement | undefined = $state();
  $effect(() => {
    firstField?.focus();
  });

  /** Everything the server would refuse, refused here first and by name. */
  function validate(): { error: string } | { ok: InvokeRequest } {
    if (!id.trim()) return { error: 'An execution id is required.' };
    if (id.includes(':')) {
      return {
        error: "An execution id must not contain ':' — that separates a promise from its lineage."
      };
    }
    if (!func.trim()) return { error: 'A function name is required.' };
    if (!target.trim()) {
      return { error: 'A target is required — without one, nothing would pick the work up.' };
    }

    let parsedArgs: unknown;
    try {
      parsedArgs = JSON.parse(args.trim() || '[]');
    } catch (e) {
      return { error: `Arguments are not valid JSON: ${(e as Error).message}` };
    }
    if (!Array.isArray(parsedArgs)) {
      return { error: 'Arguments must be a JSON array, e.g. [5, "two"].' };
    }

    const v = Number(version);
    if (!Number.isInteger(v) || v < 1) {
      return { error: 'Version must be a positive whole number.' };
    }

    const t = parseDuration(timeout);
    if ('error' in t) return t;
    // A promise whose deadline is its birth is one the server refuses: a
    // pending promise must be created strictly before it times out.
    if (t.ms <= 0) return { error: 'Timeout must be longer than zero, e.g. 1h.' };

    const d = delay.trim() ? parseDuration(delay) : { ms: 0 };
    if ('error' in d) return d;

    return {
      ok: {
        id: id.trim(),
        func: func.trim(),
        args: parsedArgs,
        version: v,
        timeoutMs: t.ms,
        target: target.trim(),
        delayMs: d.ms
      }
    };
  }

  async function submit(event: SubmitEvent) {
    event.preventDefault();
    error = null;
    clash = null;

    const checked = validate();
    if ('error' in checked) {
      error = checked.error;
      return;
    }
    const request = checked.ok;

    busy = true;
    try {
      // `promise.create` is idempotent, so creating over an existing id would
      // succeed and quietly hand back somebody else's execution. Ask first.
      const existing = await getPromise(request.id);
      if (existing) {
        clash = request.id;
        error = `An execution called ${request.id} already exists. Pick another id, or open that one.`;
        return;
      }
      await invoke(request);
      oncreated(request.id);
    } catch (e) {
      const err = e as RpcError;
      error = err instanceof RpcError ? err.message : String(e);
    } finally {
      busy = false;
    }
  }

  function onkeydown(event: KeyboardEvent) {
    if (event.key === 'Escape') {
      event.stopPropagation();
      onclose();
    }
  }
</script>

<svelte:window onkeydown={onkeydown} />

<!-- The scrim closes on click; the dialog stops the click from reaching it. -->
<!-- svelte-ignore a11y_click_events_have_key_events, a11y_no_static_element_interactions -->
<div class="scrim" onclick={onclose}>
  <div
    class="dialog card"
    role="dialog"
    aria-modal="true"
    aria-label="Invoke a function"
    tabindex="-1"
    onclick={(e) => e.stopPropagation()}
  >
    <form onsubmit={submit}>
      <div class="head">
        <div class="title">Invoke</div>
        <div class="grow"></div>
        <button type="button" class="close" onclick={onclose} aria-label="Close">✕</button>
      </div>
      <div class="fields">
        <label>
          <span>Execution id</span>
          <input
            bind:this={firstField}
            bind:value={id}
            name="id"
            placeholder="checkout.order-8842"
            spellcheck="false"
            autocomplete="off"
          />
        </label>

        <label>
          <span>Function</span>
          <input
            bind:value={func}
            name="func"
            placeholder="processCheckout"
            spellcheck="false"
            autocomplete="off"
          />
        </label>

        <label>
          <span>Arguments</span>
          <textarea bind:value={args} name="args" rows="3" spellcheck="false" placeholder="[]"></textarea>
          <em>A JSON array, passed to the function in order.</em>
        </label>

        <label>
          <span>Target</span>
          <input bind:value={target} name="target" spellcheck="false" autocomplete="off" />
          <em>Where the work is dispatched, e.g. <span class="mono">poll://any@default</span>.</em>
        </label>

        <div class="row">
          <label>
            <span>Timeout</span>
            <input bind:value={timeout} name="timeout" spellcheck="false" autocomplete="off" />
          </label>
          <label>
            <span>Delay</span>
            <input bind:value={delay} name="delay" placeholder="(none)" spellcheck="false" autocomplete="off" />
          </label>
          <label>
            <span>Version</span>
            <input bind:value={version} name="version" spellcheck="false" autocomplete="off" />
          </label>
        </div>
      </div>

      {#if error}
        <div class="error">
          {error}
          {#if clash}
            <button type="button" class="link" onclick={() => oncreated(clash!)}>
              Open {clash}
            </button>
          {/if}
        </div>
      {/if}

      <div class="actions">
        <button type="submit" class="btn-accent submit" disabled={busy}>
          {busy ? 'Invoking…' : 'Invoke'}
        </button>
        <button type="button" class="secondary" onclick={onclose}>Cancel</button>
      </div>
    </form>
  </div>
</div>

<style>
  .scrim {
    position: fixed;
    inset: 0;
    background: rgba(8, 10, 14, 0.38);
    display: flex;
    align-items: flex-start;
    justify-content: center;
    padding: 8vh 20px 20px;
    z-index: 20;
    overflow: auto;
  }
  .dialog {
    width: 520px;
    max-width: 100%;
    padding: 22px;
  }
  .head {
    display: flex;
    align-items: center;
    margin-bottom: 20px;
  }
  .title {
    font-size: 15px;
    font-weight: 600;
    letter-spacing: -0.012em;
  }
  .grow {
    flex: 1;
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
  .fields {
    display: flex;
    flex-direction: column;
    gap: 14px;
  }
  .row {
    display: flex;
    gap: 12px;
  }
  .row label {
    flex: 1;
    min-width: 0;
  }
  label {
    display: flex;
    flex-direction: column;
    gap: 6px;
  }
  /* The direct child only: a `mono` span inside a hint is not a field label,
     and uppercasing an address makes it a different address. */
  label > span {
    font-size: 12px;
    color: var(--faint);
    font-family: var(--mono);
    letter-spacing: 0.08em;
    text-transform: uppercase;
  }
  label em {
    font-size: 12px;
    color: var(--faint);
    font-style: normal;
    text-wrap: pretty;
  }
  input,
  textarea {
    border: 1px solid var(--line2);
    background: var(--bg);
    color: var(--text);
    border-radius: 6px;
    padding: 9px 11px;
    font-family: var(--mono);
    font-size: 13px;
    width: 100%;
  }
  textarea {
    resize: vertical;
    line-height: 1.6;
  }
  input:focus,
  textarea:focus {
    border-color: var(--dim);
    outline: none;
  }

  .error {
    margin-top: 16px;
    font-size: 12.5px;
    color: var(--err-fg);
    background: var(--err-bg);
    border-radius: 6px;
    padding: 9px 12px;
    text-wrap: pretty;
  }
  .link {
    display: block;
    margin-top: 7px;
    border: 0;
    background: transparent;
    padding: 0;
    color: var(--err-fg);
    font-size: 12.5px;
    text-decoration: underline;
    cursor: pointer;
  }

  .actions {
    display: flex;
    gap: 9px;
    margin-top: 22px;
  }
  /* The same padding as the secondary beside it; `.btn-accent` carries the
     colour. */
  .submit {
    padding: 8px 14px;
    font-size: 13px;
    font-weight: 500;
  }
  .secondary {
    border: 1px solid var(--line2);
    background: transparent;
    color: var(--dim);
    border-radius: 6px;
    padding: 8px 14px;
    font-size: 13px;
    cursor: pointer;
  }
  .secondary:hover {
    color: var(--text);
  }

  @media (max-width: 860px) {
    .scrim {
      padding: 4vh 12px 12px;
    }
    .row {
      flex-direction: column;
      gap: 14px;
    }
  }
</style>

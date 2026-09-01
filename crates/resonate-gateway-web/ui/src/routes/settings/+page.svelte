<script lang="ts">
  import { setPage } from '$lib/shell.svelte';
  import { rpc, RpcError } from '$lib/api';
  import {
    settings,
    save,
    reset,
    DEFAULTS,
    parseInterval,
    formatInterval
  } from '$lib/settings.svelte';

  /**
   * Where the console reads from, and how often.
   *
   * Everything here lives in this browser. `Test & save` is not a formality:
   * it makes one real request with the values as typed, so a bad URL or a
   * rejected token is found here rather than as an empty list two screens away.
   */

  let serverUrl = $state(settings.serverUrl);
  let token = $state(settings.token);
  let interval = $state(formatInterval(settings.pollMs));

  let testing = $state(false);
  let result = $state<{ ok: boolean; message: string } | null>(null);

  setPage({ title: 'Settings', searchable: false });

  async function testAndSave() {
    const pollMs = parseInterval(interval);
    if (pollMs === null) {
      result = { ok: false, message: 'Poll interval must be between 1s and 10m — e.g. 5s, 500ms is too fast.' };
      return;
    }
    testing = true;
    result = null;
    // Save first, then test: the client reads the live settings, so this is
    // what "test these values" has to mean.
    const previous = { ...settings };
    save({ serverUrl, token, pollMs });
    try {
      await rpc('ui.executions.search', { limit: 1 });
      result = { ok: true, message: 'Connected. The console is reading from this server.' };
    } catch (e) {
      const err = e as RpcError;
      save(previous);
      result = {
        ok: false,
        message: `${err.message} — settings left unchanged.`
      };
    } finally {
      testing = false;
    }
  }

  function restore() {
    reset();
    serverUrl = DEFAULTS.serverUrl;
    token = DEFAULTS.token;
    interval = formatInterval(DEFAULTS.pollMs);
    result = null;
  }
</script>

<section>
  <div class="card">
    <div class="title">Connection</div>
    <p>
      The console talks to a Resonate server through the same protocol a worker speaks, on the
      console's own route. Leave the server URL empty to read from the server that served this
      page — which is the embedded case, and needs no configuration at all.
    </p>

    <div class="fields">
      <label>
        <span>Server URL</span>
        <input
          bind:value={serverUrl}
          placeholder="(this server)"
          spellcheck="false"
          autocomplete="off"
        />
        <em>Another server's origin, e.g. http://localhost:8001. It must also serve the console, and allow this origin.</em>
      </label>

      <label>
        <span>Auth token</span>
        <input
          bind:value={token}
          type="password"
          placeholder="(none)"
          spellcheck="false"
          autocomplete="off"
        />
        <em>Sent as the envelope's auth field. Only needed when the server is started with auth configured.</em>
      </label>

      <label>
        <span>Poll interval</span>
        <input bind:value={interval} spellcheck="false" autocomplete="off" />
        <em>How often the list screens re-read their first page. 1s to 10m.</em>
      </label>
    </div>

    <div class="actions">
      <button class="primary" onclick={testAndSave} disabled={testing}>
        {testing ? 'Testing…' : 'Test & save'}
      </button>
      <button class="secondary" onclick={restore}>Reset</button>
    </div>

    {#if result}
      <div class="result" class:bad={!result.ok}>{result.message}</div>
    {/if}
  </div>
</section>

<style>
  section {
    padding: 24px 26px 40px;
    max-width: 660px;
  }
  .card {
    padding: 22px;
  }
  .title {
    font-size: 15px;
    font-weight: 600;
    letter-spacing: -0.012em;
    margin-bottom: 5px;
  }
  p {
    font-size: 13px;
    color: var(--dim);
    margin: 0 0 20px;
    max-width: 52ch;
    text-wrap: pretty;
    line-height: 1.55;
  }
  .fields {
    display: flex;
    flex-direction: column;
    gap: 14px;
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
    max-width: 52ch;
    text-wrap: pretty;
  }
  input {
    border: 1px solid var(--line2);
    background: var(--bg);
    color: var(--text);
    border-radius: 6px;
    padding: 9px 11px;
    font-family: var(--mono);
    font-size: 13px;
  }
  input:focus {
    border-color: var(--dim);
  }

  .actions {
    display: flex;
    gap: 9px;
    margin-top: 22px;
  }
  .primary {
    border: 1px solid var(--accent);
    background: var(--accent);
    color: #fff;
    border-radius: 6px;
    padding: 8px 14px;
    font-size: 13px;
    font-weight: 500;
    cursor: pointer;
  }
  .primary[disabled] {
    opacity: 0.6;
    cursor: default;
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

  .result {
    margin-top: 16px;
    font-size: 12.5px;
    color: var(--ok-fg);
    background: var(--ok-bg);
    border-radius: 6px;
    padding: 9px 12px;
    text-wrap: pretty;
  }
  .result.bad {
    color: var(--err-fg);
    background: var(--err-bg);
  }

  @media (max-width: 860px) {
    section {
      padding: 16px 14px 40px;
    }
  }
</style>

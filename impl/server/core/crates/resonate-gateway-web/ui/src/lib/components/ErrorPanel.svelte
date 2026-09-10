<script lang="ts">
  /**
   * A request that did not land, said plainly, with the way back.
   *
   * The server's own words: a `ui.*` refusal carries a code and a message, and
   * repeating them beats a generic apology when the fix is "your cursor is
   * stale" or "no such execution".
   */
  let {
    title = 'Could not load',
    message,
    code,
    onretry
  }: { title?: string; message: string; code?: string; onretry?: () => void } = $props();
</script>

<div class="panel">
  <div class="title">{title}</div>
  <div class="message">{message}</div>
  {#if code}<div class="code">{code}</div>{/if}
  {#if onretry}
    <button class="btn" onclick={onretry}>Try again</button>
  {/if}
</div>

<style>
  .panel {
    border: 1px solid var(--err-bg);
    background: var(--err-bg);
    border-radius: 8px;
    padding: 16px 18px;
    max-width: 60ch;
  }
  .title {
    font-size: 13.5px;
    font-weight: 600;
    color: var(--err-fg);
    margin-bottom: 5px;
  }
  .message {
    font-size: 13px;
    color: var(--err-fg);
    text-wrap: pretty;
  }
  .code {
    font-family: var(--mono);
    font-size: 11.5px;
    color: var(--err-fg);
    opacity: 0.75;
    margin-top: 7px;
  }
  .btn {
    margin-top: 13px;
  }
</style>

<script lang="ts">
  import { clock } from '$lib/format';

  /**
   * The poll failed but the last good answer is still on screen.
   *
   * Holding the data and labelling it beats blanking the screen: an operator
   * watching a failing execution needs what they were looking at, plus the fact
   * that it stopped being live.
   */
  let { since, message }: { since: number; message: string } = $props();
</script>

<div class="stale">
  <span class="dot ring"></span>
  <span>Showing data from {clock(since)} — the last refresh failed: {message}</span>
</div>

<style>
  .stale {
    display: flex;
    align-items: center;
    gap: 9px;
    padding: 9px 26px;
    border-bottom: 1px solid var(--line);
    background: var(--warn-bg);
    color: var(--warn-fg);
    font-size: 12.5px;
  }
  .dot {
    border-color: var(--warn-fg);
  }
</style>

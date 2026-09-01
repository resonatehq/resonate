<script lang="ts">
  import { STATE_LABEL, type PromiseState } from '$lib/types';

  /**
   * Dot + word. No pills, no badges, no uppercase.
   *
   * Three visual channels and no more: hollow for unsettled, solid for settled,
   * red for rejected. Timed out and canceled are solid like resolved — the word
   * is what disambiguates them, which is why the word is never dropped.
   */
  let { state, size = 13 }: { state: PromiseState; size?: number } = $props();

  const shape = $derived(state === 'pending' ? 'ring' : 'disc');
  const err = $derived(state === 'rejected' ? ' err' : '');
</script>

<span class="status">
  <span class="dot {shape}{err}"></span>
  <span class="word" style="font-size:{size}px">{STATE_LABEL[state]}</span>
</span>

<style>
  .status {
    display: flex;
    align-items: center;
    gap: 9px;
    min-width: 0;
  }
  .word {
    color: var(--text);
    white-space: nowrap;
  }
</style>

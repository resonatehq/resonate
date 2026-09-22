<script lang="ts">
  import type { PromiseState } from '$lib/types';

  /** The glyph on its own, for rows that carry their own label. */
  let { state }: { state: PromiseState } = $props();
  const shape = $derived(state === 'pending' ? 'ring' : 'disc');
  const err = $derived(state === 'rejected' ? ' err' : '');
</script>

<span class="dot {shape}{err}"></span>

<script lang="ts">
  /** The first read of a screen. Later reads hold the previous answer instead. */
  let { what = 'Loading…' }: { what?: string } = $props();
</script>

<div class="loading">{what}</div>

<style>
  .loading {
    padding: 48px 26px;
    font-size: 13px;
    color: var(--faint);
    animation: pulse 1.4s ease-in-out infinite;
  }
  @keyframes pulse {
    0%,
    100% {
      opacity: 1;
    }
    50% {
      opacity: 0.4;
    }
  }
</style>
